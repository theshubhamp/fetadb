package plan

import (
	"fetadb/pkg/internal"
	"fetadb/pkg/kv"
	"fetadb/pkg/kv/encoding"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type SeqScan struct {
	DB      *badger.DB
	TableID uint64
}

func (s SeqScan) Do() (internal.DataFrame, error) {
	results := internal.DataFrame{}
	return results, s.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		columns := map[uint64]*internal.Column{}

		prefix := kv.NewKey().TableID(s.TableID).IndexID(internal.DefaultIndex)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			_, _, _, columnID := kv.Key(item.Key()).Decode()

			column, ok := columns[columnID]
			if !ok {
				column = &internal.Column{
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
