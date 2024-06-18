package plan

import (
	"fetadb/pkg/util"
)

type Node interface {
	Do() (util.DataFrame, error)
}
