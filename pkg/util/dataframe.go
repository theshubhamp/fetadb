package util

import (
	"cmp"
	"fmt"
	"reflect"
	"sort"
)

type DataFrame struct {
	Columns []Column
	sort    *Sort
}

type Sort struct {
	Columns []string
	Order   []SortOrder
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
		column := df.GetColumn(columnName)

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
	leftValue, leftOk := NewNumber(left)
	rightValue, rightOk := NewNumber(right)
	if !leftOk || !rightOk {
		return false
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
