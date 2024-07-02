package expr

import (
	"fmt"
	"strings"
)

type EvaluationContext interface {
	LookupColumnRef(ref ColumnRef) (any, error)
}

type Expression interface {
	Evaluate(ec EvaluationContext) (any, error)
	String() string
}

type Equals struct {
	Left  Expression
	Right Expression
}

func (e Equals) String() string {
	return fmt.Sprintf("%v = %v", e.Left.String(), e.Right.String())
}

type ColumnRef []string

func (c ColumnRef) Evaluate(ec EvaluationContext) (any, error) {
	if ec == nil {
		return nil, fmt.Errorf("cannot evaluate column ref without evaluation context")
	}

	return ec.LookupColumnRef(c)
}

func (c ColumnRef) Name() string {
	return c[len(c)-1]
}

func (c ColumnRef) Names() []string {
	return c
}

func (c ColumnRef) String() string {
	return strings.Join(c, ".")
}

type Literal struct {
	Value any
}

func (l Literal) Evaluate(ec EvaluationContext) (any, error) {
	return l.Value, nil
}

func (l Literal) String() string {
	return fmt.Sprintf("%v", l.Value)
}
