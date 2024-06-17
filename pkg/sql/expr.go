package sql

import (
	"fmt"
	"strings"
)

type Expression interface {
	String() string
}

type Equals struct {
	Left  Expression
	Right Expression
}

func (e Equals) String() string {
	return fmt.Sprintf("%v = %v", e.Left.String(), e.Right.String())
}

type ColumnRef struct {
	Names []string
}

func (c ColumnRef) String() string {
	return strings.Join(c.Names, ".")
}

type Literal struct {
	Value any
}

func (l Literal) String() string {
	return fmt.Sprintf("%v", l.Value)
}
