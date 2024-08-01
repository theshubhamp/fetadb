package stmt

import (
	"fetadb/pkg/kv"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestCreateTable(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.Nil(t, err)

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
	require.Nil(t, err)

	tx := db.NewTransaction(false)
	defer tx.Discard()

	_, err = tx.Get(kv.TableName("test"))
	require.Nil(t, err)
}

func TestGetTable(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.Nil(t, err)

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
	require.Nil(t, err)

	table, err := GetTableByName(db, "test")
	require.Nil(t, err)
	require.Equal(t, "test", table.Name)
	require.Len(t, table.Columns, 1)
	require.Equal(t, int64(0), table.Columns[0].ID)
	require.Equal(t, "test", table.Columns[0].Name)
	require.Equal(t, reflect.String, table.Columns[0].Type)
	require.False(t, table.Columns[0].NonNull)
}
