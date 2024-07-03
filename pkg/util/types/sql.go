package types

type SortOrder int

const (
	SortAsc SortOrder = iota
	SortDesc
)

type JoinType uint64

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
)
