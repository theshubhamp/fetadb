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
			columnRef := target.DefaultColumnRef
			if target.Name != "" {
				columnRef = dataframe.NewColumnRef(target.Name)
			}

			result.AppendColumn(&dataframe.Column{
				ID:       columnID,
				TableRef: columnRef.TableRef(),
				Name:     columnRef.Column(),
			})
			columnID++
		}

		for rowIdx := range numRows {
			for colIdx, column := range result.Columns() {
				columnRef := r.Targets[colIdx].DefaultColumnRef

				var evaluated any
				if precomputedColumn := childResult.GetColumnRef(columnRef); precomputedColumn != nil {
					evaluated = precomputedColumn.Get(rowIdx)
				} else {
					evaluated, err = r.Targets[colIdx].Value.Evaluate(RowEvaluationContext{
						DFS:  []*dataframe.DataFrame{childResult},
						Rows: []int{rowIdx},
					})
					if err != nil {
						return nil, err
					}
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
	Targets []stmt.Target
	Child   Node
}

func (a Aggregate) Do(db *badger.DB) (*dataframe.DataFrame, error) {
	childResult, err := a.Child.Do(db)
	if err != nil {
		return nil, err
	}

	if childResult.RowCount() == 0 {
		return childResult, nil
	}

	evaluatedColumns := []*dataframe.Column{}
	for _, target := range a.Targets {
		if _, ok := target.Value.(expr.AggCall); !ok {
			continue
		}

		evaluatedColumns = append(evaluatedColumns, &dataframe.Column{
			ID:       0,
			Name:     target.DefaultColumnRef.Column(),
			TableRef: target.DefaultColumnRef.TableRef(),
		})
	}

	result := dataframe.NewDataFrame()
	result.IncludeColumns(childResult)

	columnGroup := childResult.GetColumnRef(util.MetaColumnGroup)
	lastColumnGroup := columnGroup.Get(0)
	lastRow := []any{}
	lastGeneratedRow := []any{}
	for row := range childResult.RowCount() {
		currentColumnGroup := columnGroup.Get(row)

		if lastColumnGroup != currentColumnGroup {
			lastColumnGroup = currentColumnGroup
			for col, cell := range lastRow {
				result.GetColumn(col).Append(cell)
			}
			for col, cell := range lastGeneratedRow {
				evaluatedColumns[col].Append(cell)
			}
			for _, target := range a.Targets {
				if aggCall, ok := target.Value.(expr.AggCall); ok {
					aggCall.Instance.Agg.Reset()
				}
			}
		}

		lastRow = []any{}
		lastGeneratedRow = []any{}
		for _, column := range childResult.Columns() {
			lastRow = append(lastRow, column.Get(row))
		}

		for _, target := range a.Targets {
			if _, ok := target.Value.(expr.AggCall); !ok {
				continue
			}

			evaluated, err := target.Value.Evaluate(RowEvaluationContext{
				DFS:  []*dataframe.DataFrame{childResult},
				Rows: []int{row},
			})
			if err != nil {
				return nil, err
			}
			lastGeneratedRow = append(lastGeneratedRow, evaluated)
		}
	}

	for col, cell := range lastRow {
		result.GetColumn(col).Append(cell)
	}
	for col, cell := range lastGeneratedRow {
		evaluatedColumns[col].Append(cell)
	}

	for _, evaluatedColumn := range evaluatedColumns {
		result.AppendColumn(evaluatedColumn)
	}

	return result, nil
}
