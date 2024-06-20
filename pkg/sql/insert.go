package sql

import (
	"fetadb/pkg/kv"
	"fetadb/pkg/kv/encoding"
	"fetadb/pkg/sql/dd"
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"reflect"
)

type Insert struct {
	Table  TargetTable
	Column []RequestedColumn
	Values [][]expr.Expression
}

type TargetTable struct {
	Catalog string
	Schema  string
	Rel     string
	Alias   string
}

type RequestedColumn struct {
	Name string
}

func prefixEmpty(txn *badger.Txn, prefix []byte) bool {
	itOpt := badger.DefaultIteratorOptions
	itOpt.PrefetchValues = false

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); {
		it.Close()
		return false
	}

	return true
}

func InsertTable(db *badger.DB, insert Insert) error {
	table, err := GetTableByName(db, insert.Table.Rel)
	if err != nil {
		return err
	}

	columns := map[string]dd.Column{}
	primaryColumnValueIndex := -1
	columnsValuesAvailable := map[string]bool{}
	for colIdx, column := range table.Columns {
		columns[column.Name] = column
		columnsValuesAvailable[column.Name] = false
		if column.Primary {
			primaryColumnValueIndex = colIdx
		}
	}
	for _, requestedColumn := range insert.Column {
		columnsValuesAvailable[requestedColumn.Name] = true
	}

	return db.Update(func(txn *badger.Txn) error {
		for _, row := range insert.Values {
			evaluatedIndexValue := reflect.ValueOf(row[primaryColumnValueIndex].Evaluate(nil))
			if !evaluatedIndexValue.CanConvert(reflect.TypeOf(uint64(1))) {
				return fmt.Errorf("primary key value expected to be uint64, got %v", evaluatedIndexValue.Kind())
			}
			indexValue := evaluatedIndexValue.Convert(reflect.TypeOf(uint64(1))).Uint()

			if !prefixEmpty(txn, kv.NewDKey().TableID(table.ID).IndexID(util.DefaultIndex).IndexValue(indexValue)) {
				return fmt.Errorf("duplicate index value %v", indexValue)
			}

			for colIdx, requestedColumn := range insert.Column {
				key := kv.NewDKey().TableID(table.ID).IndexID(util.DefaultIndex).IndexValue(indexValue).ColumnID(columns[requestedColumn.Name].ID)

				encoded, err := encoding.Encode(row[colIdx].Evaluate(nil))
				if err != nil {
					return err
				}

				err = txn.Set(key, encoded)
				if err != nil {
					return err
				}
			}

			for columnName, valueAvailable := range columnsValuesAvailable {
				if !valueAvailable && columns[columnName].NonNull {
					return fmt.Errorf("column %v marked non null but no value supplied", columnName)
				}

				if !valueAvailable {
					key := kv.NewDKey().TableID(table.ID).IndexID(util.DefaultIndex).IndexValue(indexValue).ColumnID(columns[columnName].ID)

					encoded, err := encoding.Encode(nil)
					if err != nil {
						return err
					}

					err = txn.Set(key, encoded)
					if err != nil {
						return err
					}
				}
			}
		}

		return nil
	})
}
