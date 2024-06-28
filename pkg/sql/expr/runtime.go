package expr

import (
	"fetadb/pkg/util"
	"fmt"
	"strings"
)

func Add(left any, right any) (any, error) {
	leftValue, leftOk := util.NewNumber(left)
	rightValue, rightOk := util.NewNumber(right)
	if !leftOk || !rightOk {
		return nil, fmt.Errorf("left %v and right %v expected to be numbers", left, right)
	}

	if leftValue.IsFloat() || rightValue.IsFloat() {
		return leftValue.Float() + rightValue.Float(), nil
	} else if leftValue.IsUint() || rightValue.IsUint() {
		return leftValue.Uint() + rightValue.Uint(), nil
	} else if leftValue.IsInt() || rightValue.IsInt() {
		return leftValue.Int() + rightValue.Int(), nil
	}

	panic(fmt.Sprintf("unreachable"))
}

func Subtract(left any, right any) (any, error) {
	leftValue, leftOk := util.NewNumber(left)
	rightValue, rightOk := util.NewNumber(right)
	if !leftOk || !rightOk {
		return nil, fmt.Errorf("left %v and right %v expected to be numbers", left, right)
	}

	if leftValue.IsFloat() || rightValue.IsFloat() {
		return leftValue.Float() - rightValue.Float(), nil
	} else if leftValue.IsUint() || rightValue.IsUint() {
		return leftValue.Uint() - rightValue.Uint(), nil
	} else if leftValue.IsInt() || rightValue.IsInt() {
		return leftValue.Int() - rightValue.Int(), nil
	}

	panic(fmt.Sprintf("unreachable"))
}

func Multiply(left any, right any) (any, error) {
	leftValue, leftOk := util.NewNumber(left)
	rightValue, rightOk := util.NewNumber(right)
	if !leftOk || !rightOk {
		return nil, fmt.Errorf("left %v and right %v expected to be numbers", left, right)
	}

	if leftValue.IsFloat() || rightValue.IsFloat() {
		return leftValue.Float() * rightValue.Float(), nil
	} else if leftValue.IsUint() || rightValue.IsUint() {
		return leftValue.Uint() * rightValue.Uint(), nil
	} else if leftValue.IsInt() || rightValue.IsInt() {
		return leftValue.Int() * rightValue.Int(), nil
	}

	panic(fmt.Sprintf("unreachable"))
}

func Divide(left any, right any) (any, error) {
	leftValue, leftOk := util.NewNumber(left)
	rightValue, rightOk := util.NewNumber(right)
	if !leftOk || !rightOk {
		return nil, fmt.Errorf("left %v and right %v expected to be numbers", left, right)
	}

	if leftValue.IsFloat() || rightValue.IsFloat() {
		return leftValue.Float() / rightValue.Float(), nil
	} else if leftValue.IsUint() || rightValue.IsUint() {
		return leftValue.Uint() / rightValue.Uint(), nil
	} else if leftValue.IsInt() || rightValue.IsInt() {
		return leftValue.Int() / rightValue.Int(), nil
	}

	panic(fmt.Sprintf("unreachable"))
}

func Lower(val string) string {
	return strings.ToLower(val)
}

func Upper(val string) string {
	return strings.ToUpper(val)
}
