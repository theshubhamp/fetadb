package node

import (
	"fetadb/internal"
	"github.com/dgraph-io/badger/v4"
)

type Node interface {
	Do(db *badger.DB) (internal.DataFrame, error)
}
