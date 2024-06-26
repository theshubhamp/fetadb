package util

type DataFrame []Column

type Column struct {
	ID    uint64
	Name  string
	Items []any
}

func (df *DataFrame) GetColumn(name string) *Column {
	for _, column := range *df {
		if column.Name == name {
			return &column
		}
	}

	return nil
}
