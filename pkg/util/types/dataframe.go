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
	Columns []*Column
	sort    *Sort
}

func (df *DataFrame) GetColumn(ref ColumnRef) *Column {
	for _, column := range df.Columns {
		if ref.TableRef() == column.TableRef && column.Name == ref.Column() {
			return column
		}
	}

	return nil
}

func (df *DataFrame) Sort(s Sort) {
	df.sort = &s
	sort.Sort(df)
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

func (df *DataFrame) Len() int {
	return int(df.RowCount())
}

func (df *DataFrame) Less(i int, j int) bool {
	if reflect.ValueOf(df.sort).IsZero() {
		return false
	}

	for idx, columnName := range df.sort.Columns {
		column := df.GetColumn(NewColumnRef(columnName))

		iValue := column.Items[i]
		jValue := column.Items[j]

		if reflect.DeepEqual(iValue, jValue) {
			continue
		}

		lessThan := less(iValue, jValue)
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

	temp := []any{}
	for _, column := range df.Columns {
		temp = append(temp, column.Items[i])
	}
	for idx, column := range df.Columns {
		column.Items[i] = column.Items[j]
		column.Items[j] = temp[idx]
	}

	return
}

func less(left any, right any) bool {
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
