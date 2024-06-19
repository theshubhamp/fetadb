package kv

import "encoding/binary"

var (
	PrefixMeta = byte('\x01')
	PrefixData = byte('\x02')
)

type Key []byte

func NewKey() Key {
	return Key{}
}

func (k Key) TableID(tableID uint64) Key {
	return binary.BigEndian.AppendUint64([]byte{PrefixData}, tableID)
}

func (k Key) IndexID(indexID uint64) Key {
	return binary.BigEndian.AppendUint64(k, indexID)
}

func (k Key) IndexValue(indexValue uint64) Key {
	return binary.BigEndian.AppendUint64(k, indexValue)
}

func (k Key) ColumnID(columnID uint64) Key {
	return binary.BigEndian.AppendUint64(k, columnID)
}

func (k Key) Decode() (uint64, uint64, uint64, uint64) {
	return binary.BigEndian.Uint64(k[1:9]), binary.BigEndian.Uint64(k[9:17]), binary.BigEndian.Uint64(k[17:25]), binary.BigEndian.Uint64(k[25:33])
}
