package plan

import (
	"fetadb/pkg/util"
	"github.com/dgraph-io/badger/v4"
)

type Node interface {
	Do(db *badger.DB) (util.DataFrame, error)
}
