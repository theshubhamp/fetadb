package encoding

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

const (
	null = iota
	i8   = iota
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
	case int8:
		return binary.BigEndian.AppendUint64([]byte{i8}, EncodeMemcmpInt64(int64(t))), nil
	case int16:
		return binary.BigEndian.AppendUint64([]byte{i16}, EncodeMemcmpInt64(int64(t))), nil
	case int32:
		return binary.BigEndian.AppendUint64([]byte{i32}, EncodeMemcmpInt64(int64(t))), nil
	case int64:
		return binary.BigEndian.AppendUint64([]byte{i64}, EncodeMemcmpInt64(int64(t))), nil
	case int:
		return binary.BigEndian.AppendUint64([]byte{i64}, EncodeMemcmpInt64(int64(t))), nil
	case string:
		return append([]byte{str}, []byte(t)...), nil
	}

	return []byte{}, fmt.Errorf("unsupported type: %v", reflect.TypeOf(e).Kind())
}

func EncodeMemcmpInt64(val int64) uint64 {
	encode := uint64(val)
	if val < 0 {
		encode = ^encode
	} else {
		encode = encode | (uint64(1) << 63)
	}

	return encode
}

func Decode(value []byte) any {
	switch value[0] {
	case null:
		return nil
	case i8, i16, i32, i64:
		return DecodeMemcmpInt64(value[1:])
	case str:
		return string(value[1:])
	default:
		return nil
	}
}

func DecodeMemcmpInt64(val []byte) int64 {
	decoded := binary.BigEndian.Uint64(val[:])
	if decoded&(uint64(1)<<63) == 0 {
		decoded = ^decoded
	} else {
		decoded = decoded & ^(uint64(1) << 63)
	}
	return int64(decoded)
}
