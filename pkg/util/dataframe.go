package util

type DataFrame []Column

type Column struct {
	ID    uint64
	Items []any
}
