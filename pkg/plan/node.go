package plan

import (
	"fetadb/pkg/util/types"
	"github.com/dgraph-io/badger/v4"
)

type Node interface {
	Do(db *badger.DB) (*types.DataFrame, error)
}
