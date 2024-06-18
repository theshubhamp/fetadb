package plan

import "fetadb/pkg/sql"

func PlanSelect(selectStatement sql.Select) Node {
	if len(selectStatement.From) == 0 {
		return Result{
			Targets: selectStatement.Targets,
			Child:   nil,
		}
	}

	return nil
}
