package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util/types/dataframe"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type Aggregate struct {
}

func (a Aggregate) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	return nil, fmt.Errorf("not implemented")
}

type Append struct {
}

func (a Append) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	return nil, fmt.Errorf("not implemented")
}

type Result struct {
	Targets []stmt.Target
	Child   Node
}

func (r Result) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	if r.Child == nil {
		result := dataframe.NewDataFrame()

		columnID := uint64(0)
		for _, target := range r.Targets {
			evaluated, err := target.Value.Evaluate(nil)
			if err != nil {
				return nil, err
			}

			column := &dataframe.Column{
				ID:   columnID,
				Name: target.Value.String(),
			}
			result.AppendColumn(column)
			column.Append(evaluated)
		}

		return result, nil
	} else {
		childResult, err := r.Child.Do(db)
		if err != nil {
			return childResult, err
		}

		result := dataframe.NewDataFrame()
		numRows := childResult.RowCount()

		columnID := uint64(0)
		for _, target := range r.Targets {
			currentTableRef := ""
			currentColumnName := target.Name
			if target.Name == "" {
				if columnRef, ok := target.Value.(expr.ColumnRef); ok {
					currentTableRef = columnRef.TableRef()
					currentColumnName = columnRef.Column()
				} else {
					currentColumnName = target.Value.String()
				}
			}

			result.AppendColumn(&dataframe.Column{
				ID:       columnID,
				TableRef: currentTableRef,
				Name:     currentColumnName,
			})
			columnID++
		}

		for rowIdx := range numRows {
			for colIdx, column := range result.Columns() {
				evaluated, err := r.Targets[colIdx].Value.Evaluate(RowEvaluationContext{
					DFS:  []*dataframe.DataFrame{childResult},
					Rows: []int{rowIdx},
				})
				if err != nil {
					return nil, err
				}

				column.Append(evaluated)
			}
		}

		return result, nil
	}
}

type Sort struct {
	SortBy []stmt.SortBy
	Child  Node
}

func (s Sort) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	childResult, err := s.Child.Do(db)
	if err != nil {
		return childResult, err
	}

	spec := dataframe.Sort{
		Columns: []string{},
		Order:   []dataframe.SortOrder{},
	}

	for _, sortBy := range s.SortBy {
		spec.Columns = append(spec.Columns, sortBy.Ref.String())
		spec.Order = append(spec.Order, sortBy.Order)
	}

	childResult.Sort(spec)

	return childResult, nil
}
