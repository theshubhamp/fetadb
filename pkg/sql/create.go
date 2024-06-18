package sql

import "reflect"

type Create struct {
	Table   Table
	Columns []ColumnDef
}

type Table struct {
	Catalog string
	Schema  string
	Rel     string
	Alias   string
}

type ColumnDef struct {
	Name    string
	Type    reflect.Kind
	NotNull bool
}
