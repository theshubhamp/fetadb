package util

import (
	"fmt"
	"reflect"
)

func ToString(val any) string {
	switch v := val.(type) {
	case string:
		return v
	}

	return fmt.Sprintf("%v", val)
}

func IsError(typ reflect.Type) bool {
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	return typ.Implements(errorInterface)
}
