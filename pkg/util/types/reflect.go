package types

import "reflect"

func IsError(typ reflect.Type) bool {
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	return typ.Implements(errorInterface)
}
