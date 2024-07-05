package types

type JoinType uint64

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
)
