package expr

import (
	"fetadb/pkg/util"
	"fmt"
	"reflect"
)

func Add(left any, right any) (any, error) {
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)

	if leftValue.Kind() == reflect.String || rightValue.Kind() == reflect.String {
		return util.ToString(left) + util.ToString(right), nil
	}

	switch leftValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Int() + rightValue.Int(), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Int() + int64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Int()) + rightValue.Float(), nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Uint() + uint64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Uint() + rightValue.Uint(), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Uint()) + rightValue.Float(), nil
		}
	case reflect.Float32, reflect.Float64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Float() + float64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Float() + float64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return leftValue.Float() + rightValue.Float(), nil
		}
	}

	return nil, fmt.Errorf("operator + with operands types (%v, %v) not supported", leftValue.Kind(), rightValue.Kind())
}

func Subtract(left any, right any) (any, error) {
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)

	switch leftValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Int() - rightValue.Int(), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Int() - int64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Int()) - rightValue.Float(), nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Uint() - uint64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Uint() - rightValue.Uint(), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Uint()) - rightValue.Float(), nil
		}
	case reflect.Float32, reflect.Float64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Float() - float64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Float() - float64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return leftValue.Float() - rightValue.Float(), nil
		}
	}

	return nil, fmt.Errorf("operator - with operands types (%v, %v) not supported", leftValue.Kind(), rightValue.Kind())
}

func Multiply(left any, right any) (any, error) {
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)

	switch leftValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Int() * rightValue.Int(), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Int() * int64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Int()) * rightValue.Float(), nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Uint() * uint64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Uint() * rightValue.Uint(), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Uint()) * rightValue.Float(), nil
		}
	case reflect.Float32, reflect.Float64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Float() * float64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Float() * float64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return leftValue.Float() * rightValue.Float(), nil
		}
	}

	return nil, fmt.Errorf("operator * with operands types (%v, %v) not supported", leftValue.Kind(), rightValue.Kind())
}

func Divide(left any, right any) (any, error) {
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)

	switch leftValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Int() / rightValue.Int(), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Int() / int64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Int()) / rightValue.Float(), nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Uint() / uint64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Uint() / rightValue.Uint(), nil
		case reflect.Float32, reflect.Float64:
			return float64(leftValue.Uint()) / rightValue.Float(), nil
		}
	case reflect.Float32, reflect.Float64:
		switch rightValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return leftValue.Float() / float64(rightValue.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return leftValue.Float() / float64(rightValue.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return leftValue.Float() / rightValue.Float(), nil
		}
	}

	return nil, fmt.Errorf("operator / with operands types (%v, %v) not supported", leftValue.Kind(), rightValue.Kind())
}
