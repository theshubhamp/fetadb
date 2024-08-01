package dataframe

import (
	"fmt"
	"sort"
	"strings"
)

type Sort struct {
	Columns []string
	Order   []SortOrder
}

type Column struct {
	ID       int64
	Name     string
	TableRef string
	items    Array
}

func (c *Column) ColumnRef() ColumnRef {
	if c.TableRef == "" {
		return []string{c.Name}
	} else {
		return append(strings.Split(c.TableRef, "."), c.Name)
	}
}

func (c *Column) Get(i int) any {
	return c.items.Get(i)
}

func (c *Column) Append(val any) {
	if c.items == nil {
		c.items = NewArray(val)
		return
	}

	c.items.Append(val)
}

func (c *Column) Length() int {
	if c.items == nil {
		return 0
	}

	return c.items.Length()
}

func (c *Column) Equals(i int, j int) bool {
	return c.items.Equals(i, j)
}

func (c *Column) Less(i int, j int) bool {
	return c.items.Less(i, j)
}

func (c *Column) Swap(i int, j int) {
	c.items.Swap(i, j)
}

type ColumnRef []string

func NewColumnRef(val string) ColumnRef {
	return strings.Split(val, ".")
}

func (c ColumnRef) Catalog() string {
	if len(c)-4 >= 0 {
		return c[len(c)-4]
	}

	return ""
}

func (c ColumnRef) Schema() string {
	if len(c)-3 >= 0 {
		return c[len(c)-3]
	}

	return ""
}

func (c ColumnRef) Rel() string {
	if len(c)-2 >= 0 {
		return c[len(c)-2]
	}

	return ""
}

func (c ColumnRef) TableRef() string {
	remaining := len(c) - 1
	if remaining < 0 {
		return ""
	}

	return strings.Join(c[0:remaining], ".")
}

func (c ColumnRef) Column() string {
	if len(c)-1 >= 0 {
		return c[len(c)-1]
	}

	return ""
}

func (c ColumnRef) Names() []string {
	return c
}

func (c ColumnRef) String() string {
	return strings.Join(c, ".")
}

type DataFrame struct {
	columns     []*Column
	columnIndex map[string]int
	sort        *Sort
}

func NewDataFrame() *DataFrame {
	return &DataFrame{
		columns:     []*Column{},
		columnIndex: map[string]int{},
		sort:        nil,
	}
}

func (df *DataFrame) Columns() []*Column {
	return df.columns
}

func (df *DataFrame) GetColumn(index int) *Column {
	if len(df.columns) < index {
		return nil
	}

	return df.columns[index]
}

func (df *DataFrame) GetColumnRef(ref ColumnRef) *Column {
	for _, column := range df.columns {
		if ref.TableRef() == column.TableRef && column.Name == ref.Column() {
			return column
		}
	}
	index, ok := df.columnIndex[ref.String()]
	if !ok || len(df.columns) < index {
		return nil
	}

	return df.columns[index]
}

func (df *DataFrame) AppendColumn(column *Column) {
	columnRef := fmt.Sprintf("%v.%v", column.TableRef, column.Name)

	df.columns = append(df.columns, column)
	appendIndex := len(df.columns) - 1
	df.columnIndex[columnRef] = appendIndex
}

func (df *DataFrame) IncludeColumns(other *DataFrame) {
	for _, column := range other.columns {
		df.AppendColumn(&Column{
			ID:       column.ID,
			TableRef: column.TableRef,
			Name:     column.Name,
			items:    column.items.New(),
		})
	}
}

func (df *DataFrame) Sort(s Sort) {
	df.sort = &s
	sort.Sort(df)
}

func (df *DataFrame) RowCount() int {
	if len(df.columns) == 0 {
		return 0
	}

	return df.columns[0].Length()
}

func (df *DataFrame) ColCount() int {
	return len(df.columns)
}

func (df *DataFrame) Len() int {
	return df.RowCount()
}

func (df *DataFrame) Less(i int, j int) bool {
	if df.sort == nil {
		return false
	}

	for idx, columnName := range df.sort.Columns {
		column := df.GetColumnRef(NewColumnRef(columnName))

		if column.Equals(i, j) {
			continue
		}

		lessThan := column.Less(i, j)
		if df.sort.Order[idx] == SortDesc {
			return !lessThan
		} else {
			return lessThan
		}
	}

	return false
}

func (df *DataFrame) Swap(i int, j int) {
	if df.sort == nil {
		return
	}

	for _, column := range df.columns {
		column.Swap(i, j)
	}
}
