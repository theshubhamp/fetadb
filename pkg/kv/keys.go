package kv

import (
	"encoding/binary"
)

var (
	SeqTableID       = []byte{byte('\x01')}
	PrefixTableSpace = byte('\x02')
	PrefixData       = byte('\x03')
)

func TableName(val string) []byte {
	return append([]byte{PrefixTableSpace}, val...)
}

func TableID(val uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{PrefixTableSpace}, val)
}

type DKey []byte

func NewDKey() DKey {
	return DKey{}
}

func (k DKey) TableID(tableID uint64) DKey {
	return binary.BigEndian.AppendUint64([]byte{PrefixData}, tableID)
}

func (k DKey) IndexID(indexID uint64) DKey {
	return binary.BigEndian.AppendUint64(k, indexID)
}

func (k DKey) IndexValue(indexValue uint64) DKey {
	return binary.BigEndian.AppendUint64(k, indexValue)
}

func (k DKey) ColumnID(columnID uint64) DKey {
	return binary.BigEndian.AppendUint64(k, columnID)
}

func (k DKey) Decode() (uint64, uint64, uint64, uint64) {
	return binary.BigEndian.Uint64(k[1:9]), binary.BigEndian.Uint64(k[9:17]), binary.BigEndian.Uint64(k[17:25]), binary.BigEndian.Uint64(k[25:33])
}
