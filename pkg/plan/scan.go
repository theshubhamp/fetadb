package plan

import (
	"fetadb/pkg/kv"
	"fetadb/pkg/kv/encoding"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type SeqScan struct {
	TableName string
}

func (s SeqScan) Do(db *badger.DB) (*util.DataFrame, error) {
	table, err := stmt.GetTableByName(db, s.TableName)
	if err != nil {
		return nil, err
	}

	columns := map[uint64]*util.Column{}
	for _, column := range table.Columns {
		columns[column.ID] = &util.Column{
			ID:    column.ID,
			Name:  column.Name,
			Items: []any{},
		}
	}

	results := util.DataFrame{}
	return &results, db.View(func(txn *badger.Txn) error {
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
				column.Items = append(column.Items, encoding.Decode(val))
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to get value for column %v: %v", columnID, err)
			}
		}

		for _, value := range columns {
			results.Columns = append(results.Columns, *value)
		}
		return nil
	})
}
