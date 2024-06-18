package sql

import "fetadb/pkg/sql/expr"

type Select struct {
	Targets []Target
	From    []From
	Where   expr.Expression
	Having  expr.Expression
}

type From struct {
	Catalog string
	Schema  string
	Rel     string
	Alias   string
}

type Target struct {
	Name  string
	Value expr.Expression
}
