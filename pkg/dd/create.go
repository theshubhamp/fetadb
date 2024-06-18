package dd

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fetadb/pkg/kv/encoding"
	"fetadb/pkg/sql"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

func CreateTable(db *badger.DB, create sql.Create) error {
	_, err := GetTableByName(db, create.Table.Rel)
	if err == nil {
		return fmt.Errorf("duplicate table: %v", create.Table.Rel)
	}

	tableId := uint64(0)

	columns := []Column{}
	columnId := 0
	for _, columnDef := range create.Columns {
		columns = append(columns, Column{
			ID:      uint64(columnId),
			Name:    columnDef.Name,
			Type:    columnDef.Type,
			NonNull: columnDef.NotNull,
		})
		columnId++
	}

	buffer := bytes.NewBuffer([]byte{})
	err = gob.NewEncoder(buffer).Encode(&Table{
		ID:      tableId,
		Name:    create.Table.Rel,
		Columns: columns,
	})
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(create.Table.Rel), encoding.Encode(tableId))
		if err != nil {
			return err
		}

		return txn.Set(binary.BigEndian.AppendUint64([]byte{}, tableId), buffer.Bytes())
	})
}

func GetTableByName(db *badger.DB, name string) (Table, error) {
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
		return Table{}, err
	}

	return GetTableByID(db, tableId)
}

func GetTableByID(db *badger.DB, id uint64) (Table, error) {
	table := Table{}

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
