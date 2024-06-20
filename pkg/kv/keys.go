package kv

import "encoding/binary"

var (
	PrefixMeta = byte('\x01')
	PrefixData = byte('\x02')
)

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
