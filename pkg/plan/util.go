package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
)

type RowEvaluationContext struct {
	DF  util.DataFrame
	Row uint64
}

func (r RowEvaluationContext) LookupColumnRef(ref expr.ColumnRef) any {
	for _, column := range r.DF {
		if column.Name == ref.Names[0] {
			return column.Items[r.Row]
		}
	}

	return nil
}
