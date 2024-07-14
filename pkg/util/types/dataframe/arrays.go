package dataframe

import (
	"cmp"
	"fmt"
)

type Array interface {
	New() Array
	Length() int
	Get(i int) any
	Append(val any)
	Equals(i int, j int) bool
	Less(i int, j int) bool
	Swap(i int, j int)
}

func NewArray(val any) Array {
	switch val.(type) {
	case string:
		array := NewArrayOrdered[string]()
		array.Append(val)
		return &array
	case bool:
		array := NewArrayBool()
		array.Append(val)
		return &array
	case int64:
		array := NewArrayOrdered[int64]()
		array.Append(val)
		return &array
	case uint64:
		array := NewArrayOrdered[uint64]()
		array.Append(val)
		return &array
	case float64:
		array := NewArrayOrdered[float64]()
		array.Append(val)
		return &array
	default:
		array := NewArrayAny()
		array.Append(val)
		return &array
	}
}

type items[T comparable] struct {
	values []T
	nulls  map[int]bool
}

func (it *items[T]) Length() int {
	return len(it.values)
}

func (it *items[T]) Get(i int) any {
	if has, ok := it.nulls[i]; ok && has {
		return nil
	}

	return it.values[i]
}

func (it *items[T]) Append(val any) {
	if val == nil {
		it.nulls[len(it.values)] = true
		val = *new(T)
	}

	it.values = append(it.values, val.(T))
}

func (it *items[T]) Equals(i int, j int) bool {
	return it.values[i] == it.values[j]
}

func (it *items[T]) Swap(i int, j int) {
	it.values[i], it.values[j] = it.values[j], it.values[i]
	it.nulls[i], it.nulls[j] = it.nulls[j], it.nulls[i]
}

type ArrayOrdered[T cmp.Ordered] struct {
	*items[T]
}

func NewArrayOrdered[T cmp.Ordered]() ArrayOrdered[T] {
	return ArrayOrdered[T]{items: &items[T]{values: []T{}, nulls: map[int]bool{}}}
}

func (a *ArrayOrdered[T]) New() Array {
	created := NewArrayOrdered[T]()
	return &created
}

func (a *ArrayOrdered[T]) Less(i int, j int) bool {
	return a.values[i] < a.values[j]
}

type ArrayBool struct {
	*items[bool]
}

func NewArrayBool() ArrayBool {
	return ArrayBool{items: &items[bool]{values: []bool{}, nulls: map[int]bool{}}}
}

func (a *ArrayBool) New() Array {
	created := NewArrayBool()
	return &created
}

func (a *ArrayBool) Less(i int, j int) bool {
	return !a.values[i]
}

type ArrayAny struct {
	*items[any]
}

func NewArrayAny() ArrayAny {
	return ArrayAny{items: &items[any]{values: []any{}, nulls: map[int]bool{}}}
}

func (a *ArrayAny) New() Array {
	created := NewArrayAny()
	return &created
}

func (a *ArrayAny) Less(i int, j int) bool {
	return cmp.Less(fmt.Sprintf("%v", a.values[i]), fmt.Sprintf("%v", a.values[j]))
}
