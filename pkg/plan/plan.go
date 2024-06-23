package plan

import (
	"fetadb/pkg/sql/stmt"
)

func Select(selectStatement stmt.Select) Node {
	if len(selectStatement.From) == 0 {
		return Result{
			Targets: selectStatement.Targets,
			Child:   nil,
		}
	}

	if len(selectStatement.From) == 1 {
		return Result{
			Targets: selectStatement.Targets,
			Child:   SeqScan{TableName: selectStatement.From[0].Rel},
		}
	}

	return nil
}
