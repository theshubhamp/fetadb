package stmt

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util/types"
	"fetadb/pkg/util/types/dataframe"
	"strings"
)

type Select struct {
	Targets []Target
	Source  Source
	Where   expr.Expression
	Having  expr.Expression
	SortBy  []SortBy
	GroupBy []expr.ColumnRef
}

type Source interface {
	sourceMarker()
}

type Table struct {
	Catalog string
	Schema  string
	Rel     string
	Alias   string
}

func (t Table) TableRef() string {
	ref := []string{}
	if t.Catalog != "" {
		ref = append(ref, t.Catalog)
	}
	if t.Schema != "" {
		ref = append(ref, t.Schema)
	}
	if t.Rel != "" {
		ref = append(ref, t.Rel)
	}

	return strings.Join(ref, ".")
}

func (t Table) sourceMarker() {
}

type Join struct {
	Condition expr.Expression
	Type      types.JoinType
	Left      Source
	Right     Source
}

func (j Join) sourceMarker() {
}

type Target struct {
	Name             string
	Value            expr.Expression
	DefaultColumnRef dataframe.ColumnRef
}

type SortBy struct {
	Ref   expr.ColumnRef
	Order dataframe.SortOrder
}
