package plan

import (
	"fetadb/pkg/util/types/dataframe"
	"github.com/dgraph-io/badger/v4"
)

type Node interface {
	Do(db *badger.DB) (*dataframe.DataFrame, error)
}
