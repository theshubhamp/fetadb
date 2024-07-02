package stmt

import (
	"fetadb/pkg/sql/expr"
	"fetadb/pkg/util"
	"strings"
)

type Select struct {
	Targets []Target
	Source  Source
	Where   expr.Expression
	Having  expr.Expression
	SortBy  []SortBy
}

type Source interface {
	sourceMarker()
}

type From struct {
	Catalog string
	Schema  string
	Rel     string
	Alias   string
}

func (f From) TableRef() string {
	ref := []string{}
	if f.Catalog != "" {
		ref = append(ref, f.Catalog)
	}
	if f.Schema != "" {
		ref = append(ref, f.Schema)
	}
	if f.Rel != "" {
		ref = append(ref, f.Rel)
	}

	return strings.Join(ref, ".")
}

func (f From) sourceMarker() {
}

type Join struct {
	Condition expr.Expression
	Type      util.JoinType
	Left      Source
	Right     Source
}

func (j Join) sourceMarker() {
}

type Target struct {
	Name  string
	Value expr.Expression
}

type SortBy struct {
	Ref   expr.ColumnRef
	Order util.SortOrder
}
