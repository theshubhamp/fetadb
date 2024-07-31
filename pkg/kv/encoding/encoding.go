package encoding

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

const (
	null = iota
	ui8  = iota
	ui16 = iota
	ui32 = iota
	ui64 = iota
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
	case uint8:
		encode := uint64(t) | (uint64(1) << 63)
		return binary.BigEndian.AppendUint64([]byte{ui8}, encode), nil
	case uint16:
		encode := uint64(t) | (uint64(1) << 63)
		return binary.BigEndian.AppendUint64([]byte{ui16}, encode), nil
	case uint32:
		encode := uint64(t) | (uint64(1) << 63)
		return binary.BigEndian.AppendUint64([]byte{ui32}, encode), nil
	case uint64:
		encode := uint64(t) | (uint64(1) << 63)
		return binary.BigEndian.AppendUint64([]byte{ui64}, encode), nil
	case uint:
		encode := uint64(t) | (uint64(1) << 63)
		return binary.BigEndian.AppendUint64([]byte{ui64}, encode), nil
	case int8:
		encode := uint64(t)
		if t < 0 {
			encode = ^encode
		} else {
			encode = encode | (uint64(1) << 63)
		}
		return binary.BigEndian.AppendUint64([]byte{i8}, encode), nil
	case int16:
		encode := uint64(t)
		if t < 0 {
			encode = ^encode
		} else {
			encode = encode | (uint64(1) << 63)
		}
		return binary.BigEndian.AppendUint64([]byte{i16}, encode), nil
	case int32:
		encode := uint64(t)
		if t < 0 {
			encode = ^encode
		} else {
			encode = encode | (uint64(1) << 63)
		}
		return binary.BigEndian.AppendUint64([]byte{i32}, encode), nil
	case int64:
		encode := uint64(t)
		if t < 0 {
			encode = ^encode
		} else {
			encode = encode | (uint64(1) << 63)
		}
		return binary.BigEndian.AppendUint64([]byte{i64}, encode), nil
	case int:
		encode := uint64(t)
		if t < 0 {
			encode = ^encode
		} else {
			encode = encode | (uint64(1) << 63)
		}
		return binary.BigEndian.AppendUint64([]byte{i64}, encode), nil
	case string:
		return append([]byte{str}, []byte(t)...), nil
	}

	return []byte{}, fmt.Errorf("unsupported type: %v", reflect.TypeOf(e).Kind())
}

func Decode(value []byte) any {
	switch value[0] {
	case null:
		return nil
	case ui8, ui16, ui32, ui64:
		decoded := binary.BigEndian.Uint64(value[1:])
		return decoded & ^(uint64(1) << 63)
	case i8, i16, i32, i64:
		decoded := binary.BigEndian.Uint64(value[1:])
		if decoded&(uint64(1)<<63) == 0 {
			decoded = ^decoded
		} else {
			decoded = decoded & ^(uint64(1) << 63)
		}

		return int64(decoded)
	case str:
		return string(value[1:])
	}

	return nil
}
