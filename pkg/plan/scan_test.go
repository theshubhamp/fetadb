package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/sql/stmt"
	"github.com/dgraph-io/badger/v4"
	"reflect"
	"testing"
)

func TestSeqScan(t *testing.T) {
	tableName := "test"

	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	if err != nil {
		t.Errorf("failed to open db: %v", err)
		return
	}

	err = stmt.CreateTable(db, stmt.Create{
		Table: stmt.TableDef{Rel: tableName},
		Columns: []stmt.ColumnDef{
			{Name: "id", Type: reflect.Uint64, NotNull: true, Primary: true},
			{Name: "letter", Type: reflect.String, NotNull: true, Primary: false},
		},
	})
	if err != nil {
		t.Errorf("failed to create table: %v", err)
	}

	for idx, letter := range []string{"A", "B", "C", "D"} {
		err := stmt.InsertTable(db, stmt.Insert{
			Table:  stmt.TargetTable{Rel: tableName},
			Column: []stmt.RequestedColumn{{Name: "id"}, {Name: "letter"}},
			Values: [][]expr.Expression{{expr.Literal{Value: idx}, expr.Literal{Value: letter}}},
		})
		if err != nil {
			t.Errorf("failed to add column to table: %v", err)
		}
	}

	df, err := SeqScan{TableName: "test"}.Do(db)
	if err != nil {
		t.Errorf("failed to open db: %v", err)
		return
	}
	if df.ColCount() != 2 {
		t.Errorf("expected return of 1 column: %v", err)
	}
	if df.RowCount() != 4 {
		t.Errorf("expected return of 4 rows: %v", err)
	}
}
