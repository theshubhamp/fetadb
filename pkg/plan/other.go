package plan

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/sql/stmt"
	"fetadb/pkg/util"
	"fetadb/pkg/util/types/dataframe"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"strings"
)

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

type GroupBy struct {
	Refs  []expr.ColumnRef
	Child Node
}

func (g GroupBy) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	childResult, err := g.Child.Do(db)
	if err != nil {
		return nil, err
	}

	if childResult.ColCount() == 0 {
		return childResult, nil
	}

	groupColumn := &dataframe.Column{
		ID:       0,
		TableRef: util.MetaColumnGroup.TableRef(),
		Name:     util.MetaColumnGroup.Column(),
	}

	childResult.AppendColumn(groupColumn)

	groups := map[string]int{}
	lastGroup := -1
	for idx := range childResult.RowCount() {
		parts := make([]string, len(g.Refs))

		for refIdx, columnRef := range g.Refs {
			evaluated, err := columnRef.Evaluate(RowEvaluationContext{
				DFS:  []*dataframe.DataFrame{childResult},
				Rows: []int{idx},
			})
			if err != nil {
				return nil, err
			}

			parts[refIdx] = fmt.Sprintf("%v:%v", columnRef.String(), evaluated)
		}

		key := strings.Join(parts, "::")
		group := -1
		if existingGroup, found := groups[key]; !found {
			lastGroup = lastGroup + 1
			group = lastGroup
			groups[key] = group
		} else {
			group = existingGroup
		}

		groupColumn.Append(group)
	}

	childResult.Sort(dataframe.Sort{
		Columns: []string{util.MetaColumnGroup.String()},
		Order:   []dataframe.SortOrder{dataframe.SortAsc},
	})

	return childResult, nil
}

type Aggregate struct {
}

func (a Aggregate) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	return nil, fmt.Errorf("not implemented")
}
