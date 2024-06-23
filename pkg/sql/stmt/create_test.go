package stmt

import (
	"github.com/dgraph-io/badger/v4"
	"reflect"
	"testing"
)

func TestCreateTable(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	if err != nil {
		t.Errorf("failed to open db: %v", err)
		return
	}

	err = CreateTable(db, Create{
		Table: TableDef{
			Rel: "test",
		},
		Columns: []ColumnDef{
			{
				Name:    "test",
				Type:    reflect.String,
				NotNull: false,
			},
		},
	})
	if err != nil {
		t.Errorf("failed to create table: %v", err)
		return
	}

	tx := db.NewTransaction(false)
	defer tx.Discard()

	_, err = tx.Get([]byte("test"))
	if err != nil {
		t.Errorf("expected table key to be created: %v", err)
		return
	}
}

func TestGetTable(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	if err != nil {
		t.Errorf("failed to open db: %v", err)
		return
	}

	err = CreateTable(db, Create{
		Table: TableDef{
			Rel: "test",
		},
		Columns: []ColumnDef{
			{
				Name:    "test",
				Type:    reflect.String,
				NotNull: false,
			},
		},
	})
	if err != nil {
		t.Errorf("failed to create table: %v", err)
		return
	}

	table, err := GetTableByName(db, "test")
	if err != nil {
		t.Errorf("failed to get table: %v", err)
		return
	}

	if table.Name != "test" {
		t.Errorf("table name mismatch: %v", table.Name)
		return
	}
	if len(table.Columns) != 1 {
		t.Errorf("table column length mismatch: %v", len(table.Columns))
		return
	}
	if table.Columns[0].ID != 0 {
		t.Errorf("table column id mismatch: %v", table.Columns[0].ID)
		return
	}
	if table.Columns[0].Name != "test" {
		t.Errorf("table column name mismatch: %v", table.Columns[0].Name)
		return
	}
	if table.Columns[0].Type != reflect.String {
		t.Errorf("table column expected to be string: %v", table.Columns[0].Type.String())
		return
	}
	if table.Columns[0].NonNull {
		t.Errorf("table column expected non-null false")
		return
	}
}
