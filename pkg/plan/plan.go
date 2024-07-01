package plan

import (
	"fetadb/pkg/sql/stmt"
)

func Select(selectStatement stmt.Select) Node {
	var preResultNode Node

	if len(selectStatement.From) == 1 {
		preResultNode = SeqScan{TableName: selectStatement.From[0].Rel}

		if len(selectStatement.SortBy) > 0 {
			preResultNode = Sort{
				SortBy: selectStatement.SortBy,
				Child:  preResultNode,
			}
		}
	}

	return Result{
		Targets: selectStatement.Targets,
		Child:   preResultNode,
	}
}
