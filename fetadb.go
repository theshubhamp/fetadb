package main

import (
	"fmt"
	sql "github.com/pganalyze/pg_query_go/v5"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
)

func main() {
	db, err := leveldb.OpenFile("path/to/db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Put([]byte("key"), []byte("value"), nil)
	if err != nil {
		log.Fatal(err)
	}

	result, err := sql.Parse("SELECT 42")
	if err != nil {
		panic(err)
	}

	// This will output "42"
	fmt.Printf("%d\n", result.Stmts[0].Stmt.GetSelectStmt().GetTargetList()[0].GetResTarget().GetVal().GetAConst().GetIval().Ival)
}
