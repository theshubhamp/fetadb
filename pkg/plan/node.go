package plan

import (
	"fetadb/pkg/internal"
)

type Node interface {
	Do() (internal.DataFrame, error)
}
