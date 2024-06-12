package main

import (
	"fetadb/kv"
	"fetadb/kv/encoding"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type TableSchema struct {
	ID      uint64
	IndexID uint64
}

func ScanTableFull(db *badger.DB, schema TableSchema) (DataFrame, error) {
	results := DataFrame{}
	return results, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		columns := map[uint64]*Column{}

		prefix := kv.NewKey().TableID(schema.ID).IndexID(0)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			_, _, _, columnID := kv.Key(item.Key()).Decode()

			column, ok := columns[columnID]
			if !ok {
				column = &Column{
					ID:    columnID,
					Items: []any{},
				}
				columns[columnID] = column
			}

			err := item.Value(func(val []byte) error {
				column.Items = append(column.Items, encoding.Decode(val))
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to get value for column %v: %v", columnID, err)
			}
		}

		for _, value := range columns {
			results = append(results, *value)
		}
		return nil
	})
}
