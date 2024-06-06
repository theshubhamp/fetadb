package main

import (
	"fmt"
	sql "github.com/pganalyze/pg_query_go/v5"
)

func main() {
	result, err := sql.Parse("SELECT 42")
	if err != nil {
		panic(err)
	}

	// This will output "42"
	fmt.Printf("%d\n", result.Stmts[0].Stmt.GetSelectStmt().GetTargetList()[0].GetResTarget().GetVal().GetAConst().GetIval().Ival)
}
