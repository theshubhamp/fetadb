package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util"
	"github.com/dgraph-io/badger/v4"
)

type Aggregate struct {
}

func (a Aggregate) Do(db *badger.DB) (util.DataFrame, error) {
	return util.DataFrame{}, nil
}

type Append struct {
}

func (a Append) Do(db *badger.DB) (util.DataFrame, error) {
	return util.DataFrame{}, nil
}

type Result struct {
	Targets []stmt.Target
	Child   Node
}

func (r Result) Do(db *badger.DB) (util.DataFrame, error) {
	if r.Child == nil {
		result := util.DataFrame{}

		columnID := uint64(0)
		for _, target := range r.Targets {
			evaluated, err := target.Value.Evaluate(nil)
			if err != nil {
				return nil, err
			}

			result = append(result, util.Column{
				ID:    columnID,
				Name:  target.Name,
				Items: []any{evaluated},
			})
		}

		return result, nil
	} else {
		childResult, err := r.Child.Do(db)
		if err != nil {
			return childResult, err
		}

		result := util.DataFrame{}
		numRows := len(childResult[0].Items)

		columnID := uint64(0)
		for _, target := range r.Targets {
			currentColumnName := target.Name

			if column, ok := target.Value.(expr.ColumnRef); ok && target.Name == "" {
				currentColumnName = column.Names[len(column.Names)-1]
			}

			result = append(result, util.Column{
				ID:    columnID,
				Name:  currentColumnName,
				Items: []any{},
			})
			columnID++
		}

		for rowIdx := range numRows {
			for colIdx, _ := range result {
				evaluated, err := r.Targets[colIdx].Value.Evaluate(RowEvaluationContext{DF: childResult, Row: uint64(rowIdx)})
				if err != nil {
					return nil, err
				}

				result[colIdx].Items = append(result[colIdx].Items, evaluated)
			}
		}

		return result, nil
	}
}

type Sort struct {
}

func (s Sort) Do(db *badger.DB) (util.DataFrame, error) {
	return util.DataFrame{}, nil
}
