package schema

import "reflect"

type Table struct {
	ID      uint64
	Name    string
	Columns []Column
}

type Column struct {
	ID   uint64
	Name string
	Type reflect.Kind
}
