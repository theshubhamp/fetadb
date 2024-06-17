package plan

import (
	"fetadb/pkg/internal"
	"fetadb/pkg/kv"
	"fetadb/pkg/kv/encoding"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

func TestSeqScan(t *testing.T) {
	tableID := uint64(1)

	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	if err != nil {
		t.Errorf("failed to open db: %v", err)
		return
	}

	tx := db.NewTransaction(true)
	for idx, letter := range []string{"A", "B", "C", "D"} {
		err := tx.Set(kv.NewKey().TableID(tableID).IndexID(internal.DefaultIndex).IndexValue(uint64(idx)).ColumnID(0), encoding.Encode(letter))
		if err != nil {
			t.Errorf("failed to write to db: %v", err)
			tx.Discard()
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Errorf("failed to commit tx: %v", err)
		return
	}

	df, err := SeqScan{
		TableID: tableID,
	}.Do(db)
	if err != nil {
		t.Errorf("failed to open db: %v", err)
		return
	}
	if len(df) != 1 {
		t.Errorf("expected return of 1 column: %v", err)
	}
	if len(df[0].Items) != 4 {
		t.Errorf("expected return of 4 rows: %v", err)
	}
}
