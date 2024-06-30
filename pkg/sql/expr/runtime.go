package expr

import (
	"crypto/md5"
	"encoding/hex"
	"fetadb/pkg/util"
	"fmt"
	"reflect"
	"strings"
)

func Eq(left any, right any) (any, error) {
	return reflect.DeepEqual(left, right), nil
}

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

func Concat(left string, right string) string {
	return left + right
}

func Md5(val string) string {
	hash := md5.Sum([]byte(val))
	return hex.EncodeToString(hash[:])
}
