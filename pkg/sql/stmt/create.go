package stmt

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fetadb/pkg/kv/encoding"
	"fetadb/pkg/sql/dd"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"reflect"
)

type Create struct {
	Table   TableDef
	Columns []ColumnDef
}

type TableDef struct {
	Catalog string
	Schema  string
	Rel     string
	Alias   string
}

type ColumnDef struct {
	Name    string
	Type    reflect.Kind
	Primary bool
	NotNull bool
}

func CreateTable(db *badger.DB, create Create) error {
	_, err := GetTableByName(db, create.Table.Rel)
	if err == nil {
		return fmt.Errorf("duplicate table: %v", create.Table.Rel)
	}

	tableId := uint64(0)

	columns := []dd.Column{}
	columnId := 0
	for _, columnDef := range create.Columns {
		columns = append(columns, dd.Column{
			ID:      uint64(columnId),
			Name:    columnDef.Name,
			Type:    columnDef.Type,
			Primary: columnDef.Primary,
			NonNull: columnDef.NotNull,
		})
		columnId++
	}

	buffer := bytes.NewBuffer([]byte{})
	err = gob.NewEncoder(buffer).Encode(&dd.Table{
		ID:      tableId,
		Name:    create.Table.Rel,
		Columns: columns,
	})
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		encoded, err := encoding.Encode(tableId)
		if err != nil {
			return err
		}

		err = txn.Set([]byte(create.Table.Rel), encoded)
		if err != nil {
			return err
		}

		return txn.Set(binary.BigEndian.AppendUint64([]byte{}, tableId), buffer.Bytes())
	})
}

func GetTableByName(db *badger.DB, name string) (dd.Table, error) {
	tableId := uint64(0)

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(name))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			id, ok := encoding.Decode(val).(uint64)
			if !ok {
				return fmt.Errorf("table not readable into uint64")
			}
			tableId = id
			return nil
		})
	})
	if err != nil {
		return dd.Table{}, err
	}

	return GetTableByID(db, tableId)
}

func GetTableByID(db *badger.DB, id uint64) (dd.Table, error) {
	table := dd.Table{}

	return table, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(binary.BigEndian.AppendUint64([]byte{}, id))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return gob.NewDecoder(bytes.NewBuffer(val)).Decode(&table)
		})
	})
}
