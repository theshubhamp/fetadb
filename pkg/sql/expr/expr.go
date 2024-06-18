package expr

import (
	"fmt"
	"reflect"
	"strings"
)

type EvaluationContext interface {
	LookupColumnRef(ref ColumnRef) any
}

type Expression interface {
	Evaluate(ec EvaluationContext) any
	String() string
}

type Equals struct {
	Left  Expression
	Right Expression
}

func (e Equals) Evaluate(ec EvaluationContext) any {
	return reflect.DeepEqual(e.Left.Evaluate(ec), e.Right.Evaluate(ec))
}

func (e Equals) String() string {
	return fmt.Sprintf("%v = %v", e.Left.String(), e.Right.String())
}

type ColumnRef struct {
	Names []string
}

func (c ColumnRef) Evaluate(ec EvaluationContext) any {
	return ec.LookupColumnRef(c)
}

func (c ColumnRef) String() string {
	return strings.Join(c.Names, ".")
}

type Literal struct {
	Value any
}

func (l Literal) Evaluate(ec EvaluationContext) any {
	return l.Value
}

func (l Literal) String() string {
	return fmt.Sprintf("%v", l.Value)
}
