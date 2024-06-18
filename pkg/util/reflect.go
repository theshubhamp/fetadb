package util

import (
	"log"
	"reflect"
)

var typeStrKindMap = map[string]reflect.Kind{
	"bool":    reflect.Bool,
	"int":     reflect.Int,
	"int8":    reflect.Int8,
	"int16":   reflect.Int16,
	"int32":   reflect.Int32,
	"int64":   reflect.Int64,
	"uint":    reflect.Uint,
	"uint8":   reflect.Uint8,
	"uint16":  reflect.Uint16,
	"uint32":  reflect.Uint32,
	"uint64":  reflect.Uint64,
	"float32": reflect.Float32,
	"float64": reflect.Float64,
	"string":  reflect.String,
}

func LookupKind(typeStr string) reflect.Kind {
	kind, ok := typeStrKindMap[typeStr]
	if !ok {
		log.Panicf("unsupported type %v", typeStr)
	}

	return kind
}
