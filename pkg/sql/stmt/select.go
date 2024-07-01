package stmt

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
)

type Select struct {
	Targets []Target
	From    []From
	Where   expr.Expression
	Having  expr.Expression
	SortBy  []SortBy
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

type SortBy struct {
	Ref   expr.ColumnRef
	Order util.SortOrder
}
