package plan

import (
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util"
	"fmt"
	"github.com/dgraph-io/badger/v4"
)

type Aggregate struct {
}

func (a Aggregate) Do(db *badger.DB) (*util.DataFrame, error) {
	return nil, fmt.Errorf("not implemented")
}

type Append struct {
}

func (a Append) Do(db *badger.DB) (*util.DataFrame, error) {
	return nil, fmt.Errorf("not implemented")
}

type Result struct {
	Targets []stmt.Target
	Child   Node
}

func (r Result) Do(db *badger.DB) (*util.DataFrame, error) {
	if r.Child == nil {
		result := util.DataFrame{}

		columnID := uint64(0)
		for _, target := range r.Targets {
			evaluated, err := target.Value.Evaluate(nil)
			if err != nil {
				return nil, err
			}

			result.Columns = append(result.Columns, util.Column{
				ID:    columnID,
				Name:  target.Value.String(),
				Items: []any{evaluated},
			})
		}

		return &result, nil
	} else {
		childResult, err := r.Child.Do(db)
		if err != nil {
			return childResult, err
		}

		result := util.DataFrame{}
		numRows := childResult.RowCount()

		columnID := uint64(0)
		for _, target := range r.Targets {
			currentColumnName := target.Name
			if target.Name == "" {
				currentColumnName = target.Value.String()
			}

			result.Columns = append(result.Columns, util.Column{
				ID:    columnID,
				Name:  currentColumnName,
				Items: []any{},
			})
			columnID++
		}

		for rowIdx := range numRows {
			for colIdx, _ := range result.Columns {
				evaluated, err := r.Targets[colIdx].Value.Evaluate(RowEvaluationContext{DF: childResult, Row: uint64(rowIdx)})
				if err != nil {
					return nil, err
				}

				result.Columns[colIdx].Items = append(result.Columns[colIdx].Items, evaluated)
			}
		}

		return &result, nil
	}
}

type Sort struct {
	SortBy []stmt.SortBy
	Child  Node
}

func (s Sort) Do(db *badger.DB) (*util.DataFrame, error) {
	childResult, err := s.Child.Do(db)
	if err != nil {
		return childResult, err
	}

	spec := util.Sort{
		Columns: []string{},
		Order:   []util.SortOrder{},
	}

	for _, sortBy := range s.SortBy {
		spec.Columns = append(spec.Columns, sortBy.Ref.String())
		spec.Order = append(spec.Order, sortBy.Order)
	}

	childResult.Sort(spec)

	return childResult, nil
}
