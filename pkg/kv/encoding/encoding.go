package encoding

import (
	"encoding/binary"
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

func Encode(e any) []byte {
	if e == nil {
		return []byte{null}
	}

	switch t := e.(type) {
	case uint16:
		return binary.BigEndian.AppendUint16([]byte{ui16}, t)
	case uint32:
		return binary.BigEndian.AppendUint32([]byte{ui32}, t)
	case uint64:
		return binary.BigEndian.AppendUint64([]byte{ui64}, t)
	case int16:
		return binary.BigEndian.AppendUint16([]byte{i16}, uint16(t))
	case int32:
		return binary.BigEndian.AppendUint32([]byte{i32}, uint32(t))
	case int64:
		return binary.BigEndian.AppendUint64([]byte{i64}, uint64(t))
	case string:
		return append([]byte{str}, []byte(t)...)
	}

	return []byte{}
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
		return int16(binary.BigEndian.Uint64(value[1:]))
	case i32:
		return int32(binary.BigEndian.Uint64(value[1:]))
	case i64:
		return int64(binary.BigEndian.Uint64(value[1:]))
	case str:
		return string(value[1:])
	}

	return nil
}
