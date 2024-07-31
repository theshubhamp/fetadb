package util

import (
	"fmt"
	"reflect"
)

var typeStrKindMap = map[string]reflect.Kind{
	"bool":             reflect.Bool,
	"int2":             reflect.Int16,
	"smallint":         reflect.Int16,
	"int4":             reflect.Int32,
	"integer":          reflect.Int32,
	"int8":             reflect.Int64,
	"bigint":           reflect.Int64,
	"float4":           reflect.Float32,
	"real":             reflect.Float32,
	"double precision": reflect.Float64,
	"float8":           reflect.Float64,
	"string":           reflect.String,
}

func LookupKind(typeStr string) (reflect.Kind, error) {
	kind, ok := typeStrKindMap[typeStr]
	if !ok {
		return 0, fmt.Errorf("unsupported type %v", typeStr)
	}

	return kind, nil
}
