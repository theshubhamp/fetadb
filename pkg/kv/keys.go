package kv

import (
	"encoding/binary"
	"fetadb/pkg/kv/encoding"
)

var (
	SeqTableID       = []byte{byte('\x01')}
	PrefixTableSpace = byte('\x02')
	PrefixData       = byte('\x03')
)

func TableName(val string) []byte {
	return append([]byte{PrefixTableSpace}, val...)
}

func TableID(val int64) []byte {
	return binary.BigEndian.AppendUint64([]byte{PrefixTableSpace}, encoding.EncodeMemcmpInt64(val))
}

type DKey []byte

func NewDKey() DKey {
	return DKey{}
}

func (k DKey) TableID(tableID int64) DKey {
	return binary.BigEndian.AppendUint64([]byte{PrefixData}, encoding.EncodeMemcmpInt64(tableID))
}

func (k DKey) IndexID(indexID int64) DKey {
	return binary.BigEndian.AppendUint64(k, encoding.EncodeMemcmpInt64(indexID))
}

func (k DKey) IndexValue(indexValue int64) DKey {
	return binary.BigEndian.AppendUint64(k, encoding.EncodeMemcmpInt64(indexValue))
}

func (k DKey) ColumnID(columnID int64) DKey {
	return binary.BigEndian.AppendUint64(k, encoding.EncodeMemcmpInt64(columnID))
}

func (k DKey) Decode() (int64, int64, int64, int64) {
	return encoding.DecodeMemcmpInt64(k[1:9]), encoding.DecodeMemcmpInt64(k[9:17]), encoding.DecodeMemcmpInt64(k[17:25]), encoding.DecodeMemcmpInt64(k[25:33])
}
