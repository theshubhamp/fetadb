package dd

import "reflect"

type Table struct {
	ID      uint64
	Name    string
	Columns []Column
}

type Column struct {
	ID      uint64
	Name    string
	Type    reflect.Kind
	Primary bool
	NonNull bool
}

func (t Table) GetColumnByName(name string) (Column, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}

	return Column{}, false
}

func (t Table) GetColumnByID(id uint64) (Column, bool) {
	for _, column := range t.Columns {
		if column.ID == id {
			return column, true
		}
	}

	return Column{}, false
}
