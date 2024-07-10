package plan

import (
	"fetadb/pkg/sql/stmt"
	"fmt"
	"reflect"
)

func Select(selectStatement stmt.Select) (Node, error) {
	var preResultNode Node

	if selectStatement.Source != nil {
		source, err := Source(selectStatement.Source)
		if err != nil {
			return nil, err
		}
		preResultNode = source
	}

	if len(selectStatement.GroupBy) > 0 {
		preResultNode = GroupBy{
			Refs:  selectStatement.GroupBy,
			Child: preResultNode,
		}

		preResultNode = Aggregate{
			Targets: selectStatement.Targets,
			Child:   preResultNode,
		}
	}

	if len(selectStatement.SortBy) > 0 {
		preResultNode = Sort{
			SortBy: selectStatement.SortBy,
			Child:  preResultNode,
		}
	}

	return Result{
		Targets: selectStatement.Targets,
		Child:   preResultNode,
	}, nil
}

func Source(source stmt.Source) (Node, error) {
	if from, ok := source.(stmt.From); ok {
		return SeqScan{TableRef: from.TableRef()}, nil
	} else if join, ok := source.(stmt.Join); ok {
		leftSource, err := Source(join.Left)
		if err != nil {
			return nil, err
		}
		rightSource, err := Source(join.Right)
		if err != nil {
			return nil, err
		}

		return NestedJoin{Condition: join.Condition, Type: join.Type, Left: leftSource, Right: rightSource}, nil
	} else {
		return nil, fmt.Errorf("unsupported source: %v", reflect.TypeOf(source))
	}

}
