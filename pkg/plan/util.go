package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
	"fmt"
)

type RowEvaluationContext struct {
	DF  util.DataFrame
	Row uint64
}

func (r RowEvaluationContext) LookupColumnRef(ref expr.ColumnRef) (any, error) {
	for _, column := range r.DF {
		if column.Name == ref.Names[0] {
			return column.Items[r.Row], nil
		}
	}

	return nil, fmt.Errorf("column ref '%v' not found", ref.String())
}
