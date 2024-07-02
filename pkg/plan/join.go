package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type NestedJoin struct {
	Condition expr.Expression
	Type      util.JoinType
	Left      Node
	Right     Node
}

func (n NestedJoin) Do(db *badger.DB) (*util.DataFrame, error) {
	leftResult, err := n.Left.Do(db)
	if err != nil {
		return nil, err
	}

	rightResult, err := n.Right.Do(db)
	if err != nil {
		return nil, err
	}

	joinedResult := util.DataFrame{}
	for _, column := range leftResult.Columns {
		joinedResult.Columns = append(joinedResult.Columns, &util.Column{
			ID:       column.ID,
			TableRef: column.TableRef,
			Name:     column.Name,
			Items:    []any{},
		})
	}
	for _, column := range rightResult.Columns {
		joinedResult.Columns = append(joinedResult.Columns, &util.Column{
			ID:       column.ID,
			Name:     column.Name,
			TableRef: column.TableRef,
			Items:    []any{},
		})
	}

	joinedRightIndexes := map[uint64]bool{}
	for leftIdx := range leftResult.RowCount() {
		joined := false
		joinedRightIdx := -1

		for rightIdx := range rightResult.RowCount() {
			evaluated, err := n.Condition.Evaluate(RowEvaluationContext{
				DFS:  []*util.DataFrame{leftResult, rightResult},
				Rows: []uint64{leftIdx, rightIdx},
			})
			if err != nil {
				return nil, err
			}

			if evaluatedBool, ok := evaluated.(bool); ok {
				joined = evaluatedBool
				if joined {
					joinedRightIdx = int(rightIdx)
					joinedRightIndexes[uint64(joinedRightIdx)] = true
					break
				}
			}
		}

		if n.Type == util.JoinInner {
			if !joined {
				continue
			}

			for _, leftColumn := range leftResult.Columns {
				joinedResult.GetColumn(leftColumn.ColumnRef()).Append(leftColumn.Items[leftIdx])
			}
			for _, rightColumn := range rightResult.Columns {
				joinedResult.GetColumn(rightColumn.ColumnRef()).Append(rightColumn.Items[joinedRightIdx])
			}
		} else if n.Type == util.JoinLeft {
			for _, leftColumn := range leftResult.Columns {
				joinedResult.GetColumn(leftColumn.ColumnRef()).Append(leftColumn.Items[leftIdx])
			}
			if joined {
				for _, rightColumn := range rightResult.Columns {
					joinedResult.GetColumn(rightColumn.ColumnRef()).Append(rightColumn.Items[joinedRightIdx])
				}
			} else {
				for _, rightColumn := range rightResult.Columns {
					joinedResult.GetColumn(rightColumn.ColumnRef()).Append(nil)
				}
			}
		} else if n.Type == util.JoinRight {
			if joined {
				for _, leftColumn := range leftResult.Columns {
					joinedResult.GetColumn(leftColumn.ColumnRef()).Append(leftColumn.Items[leftIdx])
				}
				for _, rightColumn := range rightResult.Columns {
					joinedResult.GetColumn(rightColumn.ColumnRef()).Append(rightColumn.Items[joinedRightIdx])
				}
			}
		} else {
			return nil, fmt.Errorf("unsupported join type")
		}
	}

	if n.Type == util.JoinRight {
		for rightIdx := range rightResult.RowCount() {
			if _, ok := joinedRightIndexes[rightIdx]; !ok {
				for _, leftColumn := range leftResult.Columns {
					joinedResult.GetColumn(leftColumn.ColumnRef()).Append(nil)
				}
				for _, rightColumn := range rightResult.Columns {
					joinedResult.GetColumn(rightColumn.ColumnRef()).Append(rightColumn.Items[rightIdx])
				}
			}
		}
	}

	return &joinedResult, nil
}
