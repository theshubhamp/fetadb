package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util/types"
	"fetadb/pkg/util/types/dataframe"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type NestedJoin struct {
	Condition expr.Expression
	Type      types.JoinType
	Left      Node
	Right     Node
}

func (n NestedJoin) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	leftResult, err := n.Left.Do(db)
	if err != nil {
		return nil, err
	}

	rightResult, err := n.Right.Do(db)
	if err != nil {
		return nil, err
	}

	joinedResult := dataframe.NewDataFrame()
	joinedResult.IncludeColumns(leftResult)
	joinedResult.IncludeColumns(rightResult)

	joinedRightIndexes := map[int]bool{}
	for leftIdx := range leftResult.RowCount() {
		joined := false
		joinedRightIdx := -1

		for rightIdx := range rightResult.RowCount() {
			evaluated, err := n.Condition.Evaluate(RowEvaluationContext{
				DFS:  []*dataframe.DataFrame{leftResult, rightResult},
				Rows: []int{leftIdx, rightIdx},
			})
			if err != nil {
				return nil, err
			}

			if evaluatedBool, ok := evaluated.(bool); ok {
				joined = evaluatedBool
				if joined {
					joinedRightIdx = rightIdx
					joinedRightIndexes[joinedRightIdx] = true
					break
				}
			}
		}

		if n.Type == types.JoinInner {
			if !joined {
				continue
			}

			for _, leftColumn := range leftResult.Columns() {
				joinedResult.GetColumnRef(leftColumn.ColumnRef()).Append(leftColumn.Get(leftIdx))
			}
			for _, rightColumn := range rightResult.Columns() {
				joinedResult.GetColumnRef(rightColumn.ColumnRef()).Append(rightColumn.Get(joinedRightIdx))
			}
		} else if n.Type == types.JoinLeft {
			for _, leftColumn := range leftResult.Columns() {
				joinedResult.GetColumnRef(leftColumn.ColumnRef()).Append(leftColumn.Get(leftIdx))
			}
			if joined {
				for _, rightColumn := range rightResult.Columns() {
					joinedResult.GetColumnRef(rightColumn.ColumnRef()).Append(rightColumn.Get(joinedRightIdx))
				}
			} else {
				for _, rightColumn := range rightResult.Columns() {
					joinedResult.GetColumnRef(rightColumn.ColumnRef()).Append(nil)
				}
			}
		} else if n.Type == types.JoinRight {
			if joined {
				for _, leftColumn := range leftResult.Columns() {
					joinedResult.GetColumnRef(leftColumn.ColumnRef()).Append(leftColumn.Get(leftIdx))
				}
				for _, rightColumn := range rightResult.Columns() {
					joinedResult.GetColumnRef(rightColumn.ColumnRef()).Append(rightColumn.Get(joinedRightIdx))
				}
			}
		} else {
			return nil, fmt.Errorf("unsupported join type")
		}
	}

	if n.Type == types.JoinRight {
		for rightIdx := range rightResult.RowCount() {
			if _, ok := joinedRightIndexes[rightIdx]; !ok {
				for _, leftColumn := range leftResult.Columns() {
					joinedResult.GetColumnRef(leftColumn.ColumnRef()).Append(nil)
				}
				for _, rightColumn := range rightResult.Columns() {
					joinedResult.GetColumnRef(rightColumn.ColumnRef()).Append(rightColumn.Get(rightIdx))
				}
			}
		}
	}

	return joinedResult, nil
}
