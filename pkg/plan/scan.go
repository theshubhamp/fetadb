package plan

import (
	"fetadb/pkg/kv"
	"fetadb/pkg/kv/encoding"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util"
	"fetadb/pkg/util/types/dataframe"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type SeqScan struct {
	TableRef string
}

func (s SeqScan) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	table, err := stmt.GetTableByName(db, s.TableRef)
	if err != nil {
		return nil, err
	}

	columns := map[uint64]*dataframe.Column{}
	for _, column := range table.Columns {
		columns[column.ID] = &dataframe.Column{
			ID:       column.ID,
			Name:     column.Name,
			TableRef: s.TableRef,
		}
	}

	results := dataframe.NewDataFrame()

	return results, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := kv.NewDKey().TableID(table.ID).IndexID(util.DefaultIndex)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			_, _, _, columnID := kv.DKey(item.Key()).Decode()

			column, ok := columns[columnID]
			if !ok {
				return fmt.Errorf("column with id %v not found in table %v", columnID, table.Name)
			}

			err := item.Value(func(val []byte) error {
				column.Append(encoding.Decode(val))
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to get value for column %v: %v", columnID, err)
			}
		}

		for _, value := range columns {
			results.AppendColumn(value)
		}
		return nil
	})
}
