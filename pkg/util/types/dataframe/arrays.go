package dataframe

import (
	"cmp"
	"fmt"
)

type Array interface {
	New() Array
	Length() int
	Get(i int) any
	Append(val any) Array
	Equals(i int, j int) bool
	Less(i int, j int) bool
	Swap(i int, j int)
}

func NewArray(val any) Array {
	switch val.(type) {
	case string:
		array := NewArrayString()
		return array.Append(val)
	case bool:
		array := NewArrayBool()
		return array.Append(val)
	case int64:
		array := NewArrayInt64()
		return array.Append(val)
	case uint64:
		array := NewArrayUint64()
		return array.Append(val)
	case float64:
		array := NewArrayFloat64()
		return array.Append(val)
	default:
		array := NewArrayAny()
		return array.Append(val)
	}
}

type items[T any] struct {
	items []T
	nulls map[int]bool
}

type ArrayString items[string]

func NewArrayString() ArrayString {
	return ArrayString{items: []string{}, nulls: map[int]bool{}}
}

func (a *ArrayString) New() Array {
	created := NewArrayString()
	return &created
}

func (a *ArrayString) Length() int {
	return len(a.items)
}

func (a *ArrayString) Get(i int) any {
	if has, ok := a.nulls[i]; ok && has {
		return nil
	}

	return a.items[i]
}

func (a *ArrayString) Append(val any) Array {
	if val == nil {
		a.nulls[len(a.items)] = true
		val = ""
	}

	a.items = append(a.items, val.(string))
	return a
}

func (a *ArrayString) Equals(i int, j int) bool {
	return a.items[i] == a.items[j]
}

func (a *ArrayString) Less(i int, j int) bool {
	return a.items[i] < a.items[j]
}

func (a *ArrayString) Swap(i int, j int) {
	a.items[i], a.items[j] = a.items[j], a.items[i]
	a.nulls[i], a.nulls[j] = a.nulls[j], a.nulls[i]
}

type ArrayBool items[bool]

func NewArrayBool() ArrayBool {
	return ArrayBool{items: []bool{}, nulls: map[int]bool{}}
}

func (a *ArrayBool) New() Array {
	created := NewArrayBool()
	return &created
}

func (a *ArrayBool) Length() int {
	return len(a.items)
}

func (a *ArrayBool) Get(i int) any {
	if has, ok := a.nulls[i]; ok && has {
		return nil
	}

	return a.items[i]
}

func (a *ArrayBool) Append(val any) Array {
	if val == nil {
		a.nulls[len(a.items)] = true
		val = false
	}

	a.items = append(a.items, val.(bool))
	return a
}

func (a *ArrayBool) Equals(i int, j int) bool {
	return a.items[i] == a.items[j]
}

func (a *ArrayBool) Less(i int, j int) bool {
	return !a.items[i]
}

func (a *ArrayBool) Swap(i int, j int) {
	a.items[i], a.items[j] = a.items[j], a.items[i]
	a.nulls[i], a.nulls[j] = a.nulls[j], a.nulls[i]
}

type ArrayInt64 items[int64]

func NewArrayInt64() ArrayInt64 {
	return ArrayInt64{items: []int64{}, nulls: map[int]bool{}}
}

func (a *ArrayInt64) New() Array {
	created := NewArrayInt64()
	return &created
}

func (a *ArrayInt64) Length() int {
	return len(a.items)
}

func (a *ArrayInt64) Get(i int) any {
	if has, ok := a.nulls[i]; ok && has {
		return nil
	}

	return a.items[i]
}

func (a *ArrayInt64) Append(val any) Array {
	if val == nil {
		a.nulls[len(a.items)] = true
		val = int64(0)
	}

	a.items = append(a.items, val.(int64))
	return a
}

func (a *ArrayInt64) Equals(i int, j int) bool {
	return a.items[i] == a.items[j]
}

func (a *ArrayInt64) Less(i int, j int) bool {
	return a.items[i] < a.items[j]
}

func (a *ArrayInt64) Swap(i int, j int) {
	a.items[i], a.items[j] = a.items[j], a.items[i]
	a.nulls[i], a.nulls[j] = a.nulls[j], a.nulls[i]
}

type ArrayUint64 items[uint64]

func NewArrayUint64() ArrayUint64 {
	return ArrayUint64{items: []uint64{}, nulls: map[int]bool{}}
}

func (a *ArrayUint64) New() Array {
	created := NewArrayUint64()
	return &created
}

func (a *ArrayUint64) Length() int {
	return len(a.items)
}

func (a *ArrayUint64) Get(i int) any {
	if has, ok := a.nulls[i]; ok && has {
		return nil
	}

	return a.items[i]
}

func (a *ArrayUint64) Append(val any) Array {
	if val == nil {
		a.nulls[len(a.items)] = true
		val = uint64(0)
	}

	a.items = append(a.items, val.(uint64))
	return a
}

func (a *ArrayUint64) Equals(i int, j int) bool {
	return a.items[i] == a.items[j]
}

func (a *ArrayUint64) Less(i int, j int) bool {
	return a.items[i] < a.items[j]
}

func (a *ArrayUint64) Swap(i int, j int) {
	a.items[i], a.items[j] = a.items[j], a.items[i]
	a.nulls[i], a.nulls[j] = a.nulls[j], a.nulls[i]
}

type ArrayFloat64 items[float64]

func NewArrayFloat64() ArrayFloat64 {
	return ArrayFloat64{items: []float64{}, nulls: map[int]bool{}}
}

func (a *ArrayFloat64) New() Array {
	created := NewArrayFloat64()
	return &created
}

func (a *ArrayFloat64) Length() int {
	return len(a.items)
}

func (a *ArrayFloat64) Get(i int) any {
	if has, ok := a.nulls[i]; ok && has {
		return nil
	}

	return a.items[i]
}

func (a *ArrayFloat64) Append(val any) Array {
	if val == nil {
		a.nulls[len(a.items)] = true
		val = float64(0)
	}

	a.items = append(a.items, val.(float64))
	return a
}

func (a *ArrayFloat64) Equals(i int, j int) bool {
	return a.items[i] == a.items[j]
}

func (a *ArrayFloat64) Less(i int, j int) bool {
	return a.items[i] < a.items[j]
}

func (a *ArrayFloat64) Swap(i int, j int) {
	a.items[i], a.items[j] = a.items[j], a.items[i]
	a.nulls[i], a.nulls[j] = a.nulls[j], a.nulls[i]
}

type ArrayAny items[any]

func NewArrayAny() ArrayAny {
	return ArrayAny{items: []any{}, nulls: map[int]bool{}}
}

func (a *ArrayAny) New() Array {
	created := NewArrayAny()
	return &created
}

func (a *ArrayAny) Length() int {
	return len(a.items)
}

func (a *ArrayAny) Get(i int) any {
	if has, ok := a.nulls[i]; ok && has {
		return nil
	}

	return a.items[i]
}

func (a *ArrayAny) Append(val any) Array {
	if val == nil {
		a.nulls[len(a.items)] = true
		val = nil
	}

	a.items = append(a.items, val)
	return a
}

func (a *ArrayAny) Equals(i int, j int) bool {
	return a.items[i] == a.items[j]
}

func (a *ArrayAny) Less(i int, j int) bool {
	return cmp.Less(fmt.Sprintf("%v", a.items[i]), fmt.Sprintf("%v", a.items[j]))
}

func (a *ArrayAny) Swap(i int, j int) {
	a.items[i], a.items[j] = a.items[j], a.items[i]
	a.nulls[i], a.nulls[j] = a.nulls[j], a.nulls[i]
}
