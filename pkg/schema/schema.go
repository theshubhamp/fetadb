package schema

type Table struct {
	ID      uint64
	Name    string
	Columns []Column
}

type Column struct {
	ID   uint64
	Name string
}
