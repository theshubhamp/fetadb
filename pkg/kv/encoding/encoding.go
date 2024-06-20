package encoding

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

const (
	null = iota
	ui16 = iota
	ui32 = iota
	ui64 = iota
	i16  = iota
	i32  = iota
	i64  = iota
	str  = iota
)

func Encode(e any) ([]byte, error) {
	if e == nil {
		return []byte{null}, nil
	}

	switch t := e.(type) {
	case uint8:
		return binary.BigEndian.AppendUint16([]byte{ui16}, uint16(t)), nil
	case uint16:
		return binary.BigEndian.AppendUint16([]byte{ui16}, t), nil
	case uint32:
		return binary.BigEndian.AppendUint32([]byte{ui32}, t), nil
	case uint64:
		return binary.BigEndian.AppendUint64([]byte{ui64}, t), nil
	case uint:
		return binary.BigEndian.AppendUint64([]byte{ui64}, uint64(t)), nil
	case int8:
		return binary.BigEndian.AppendUint16([]byte{i16}, uint16(t)), nil
	case int16:
		return binary.BigEndian.AppendUint16([]byte{i16}, uint16(t)), nil
	case int32:
		return binary.BigEndian.AppendUint32([]byte{i32}, uint32(t)), nil
	case int64:
		return binary.BigEndian.AppendUint64([]byte{i64}, uint64(t)), nil
	case int:
		return binary.BigEndian.AppendUint64([]byte{i64}, uint64(t)), nil
	case string:
		return append([]byte{str}, []byte(t)...), nil
	}

	return []byte{}, fmt.Errorf("unsupported type: %v", reflect.TypeOf(e).Kind())
}

func Decode(value []byte) any {
	switch value[0] {
	case null:
		return nil
	case ui16:
		return binary.BigEndian.Uint16(value[1:])
	case ui32:
		return binary.BigEndian.Uint32(value[1:])
	case ui64:
		return binary.BigEndian.Uint64(value[1:])
	case i16:
		return int16(binary.BigEndian.Uint16(value[1:]))
	case i32:
		return int32(binary.BigEndian.Uint32(value[1:]))
	case i64:
		return int64(binary.BigEndian.Uint64(value[1:]))
	case str:
		return string(value[1:])
	}

	return nil
}
