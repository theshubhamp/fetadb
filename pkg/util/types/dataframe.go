package types

import (
	"cmp"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type Sort struct {
	Columns []string
	Order   []SortOrder
}

type Column struct {
	ID       uint64
	Name     string
	TableRef string
	Items    []any
}

func (c *Column) ColumnRef() ColumnRef {
	return append(strings.Split(c.TableRef, "."), c.Name)
}

func (c *Column) Append(val any) {
	c.Items = append(c.Items, val)
}

func (c *Column) Equals(i int, j int) bool {
	return reflect.DeepEqual(c.Items[i], c.Items[j])
}

func (c *Column) Less(i int, j int) bool {
	left := c.Items[i]
	right := c.Items[j]

	if left == nil {
		return true
	} else if right == nil {
		return false
	}

	leftValue, leftOk := NewNumber(left)
	rightValue, rightOk := NewNumber(right)
	if !leftOk || !rightOk {
		return cmp.Less(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right))
	}

	if leftValue.IsFloat() || rightValue.IsFloat() {
		return cmp.Less(leftValue.Float(), rightValue.Float())
	} else if leftValue.IsUint() || rightValue.IsUint() {
		return cmp.Less(leftValue.Uint(), rightValue.Uint())
	} else if leftValue.IsInt() || rightValue.IsInt() {
		return cmp.Less(leftValue.Int(), rightValue.Int())
	}

	return cmp.Less(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right))
}

func (c *Column) Swap(i int, j int) {
	c.Items[i], c.Items[j] = c.Items[j], c.Items[i]
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

type DataFrame struct {
	columns []*Column
	sort    *Sort
}

func (df *DataFrame) Columns() []*Column {
	return df.columns
}

func (df *DataFrame) GetColumn(ref ColumnRef) *Column {
	for _, column := range df.columns {
		if ref.TableRef() == column.TableRef && column.Name == ref.Column() {
			return column
		}
	}

	return nil
}

func (df *DataFrame) AppendColumn(column *Column) {
	df.columns = append(df.columns, column)
}

func (df *DataFrame) Sort(s Sort) {
	df.sort = &s
	sort.Sort(df)
}

func (df *DataFrame) RowCount() uint64 {
	if len(df.columns) == 0 {
		return 0
	}

	return uint64(len(df.columns[0].Items))
}

func (df *DataFrame) ColCount() uint64 {
	return uint64(len(df.columns))
}

func (df *DataFrame) Len() int {
	return int(df.RowCount())
}

func (df *DataFrame) Less(i int, j int) bool {
	if reflect.ValueOf(df.sort).IsZero() {
		return false
	}

	for idx, columnName := range df.sort.Columns {
		column := df.GetColumn(NewColumnRef(columnName))

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
	if reflect.ValueOf(df.sort).IsZero() {
		return
	}

	for _, column := range df.columns {
		column.Swap(i, j)
	}
}
