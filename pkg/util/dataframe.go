package util

type DataFrame struct {
	Columns []Column
}

type Column struct {
	ID    uint64
	Name  string
	Items []any
}

func (df *DataFrame) GetColumn(name string) *Column {
	for _, column := range df.Columns {
		if column.Name == name {
			return &column
		}
	}

	return nil
}

func (df *DataFrame) RowCount() uint64 {
	if len(df.Columns) == 0 {
		return 0
	}

	return uint64(len(df.Columns[0].Items))
}

func (df *DataFrame) ColCount() uint64 {
	return uint64(len(df.Columns))
}
