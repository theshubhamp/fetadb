package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util/types/dataframe"
	"fmt"
)

type RowEvaluationContext struct {
	DFS  []*dataframe.DataFrame
	Rows []int
}

func (r RowEvaluationContext) LookupColumnRef(ref expr.ColumnRef) (any, error) {
	for idx, df := range r.DFS {
		for _, column := range df.Columns() {
			if column.TableRef == ref.TableRef() && column.Name == ref.Column() {
				return column.Get(r.Rows[idx]), nil
			}
		}
	}

	return nil, fmt.Errorf("column ref '%v' not found", ref.String())
}
