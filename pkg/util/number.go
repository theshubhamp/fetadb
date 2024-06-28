package util

import (
	"fmt"
	"reflect"
)

type Number struct {
	val reflect.Value
}

func NewNumber(val any) (Number, bool) {
	number := Number{val: reflect.ValueOf(val)}
	return number, number.val.CanInt() || number.val.CanUint() || number.val.CanFloat()
}

func (n Number) IsInt() bool {
	return n.val.CanInt()
}

func (n Number) Int() int64 {
	if n.val.CanInt() {
		return n.val.Int()
	} else if n.val.CanUint() {
		return int64(n.val.Uint())
	} else if n.val.CanFloat() {
		return int64(n.val.Float())
	}

	panic(fmt.Sprintf("expected number, got %v", n.val.Kind()))
}

func (n Number) IsUint() bool {
	return n.val.CanUint()
}

func (n Number) Uint() uint64 {
	if n.val.CanInt() {
		return uint64(n.val.Int())
	} else if n.val.CanUint() {
		return n.val.Uint()
	} else if n.val.CanFloat() {
		return uint64(n.val.Float())
	}

	panic(fmt.Sprintf("expected number, got %v", n.val.Kind()))
}

func (n Number) IsFloat() bool {
	return n.val.CanFloat()
}

func (n Number) Float() float64 {
	if n.val.CanInt() {
		return float64(n.val.Int())
	} else if n.val.CanUint() {
		return float64(n.val.Uint())
	} else if n.val.CanFloat() {
		return n.val.Float()
	}

	panic(fmt.Sprintf("expected number, got %v", n.val.Kind()))
}
