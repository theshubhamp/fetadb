package expr

import (
	"fmt"
	"reflect"
)

type delegateFunc func(left any, right any) any

var delegates = map[string]delegateFunc{
	"=": operatorEq,
}

func NewBinaryOperator(operator string, left Expression, right Expression) BinaryOperator {
	f, _ := delegates[operator]

	return BinaryOperator{
		Operator: operator,
		delegate: f,
		Left:     left,
		Right:    right,
	}
}

type BinaryOperator struct {
	Operator string
	delegate delegateFunc
	Left     Expression
	Right    Expression
}

func (b BinaryOperator) Evaluate(ec EvaluationContext) any {
	return b.delegate(b.Left.Evaluate(ec), b.Right.Evaluate(ec))
}

func (b BinaryOperator) String() string {
	return fmt.Sprintf("%v %v %v", b.Left.String(), b.Operator, b.Right.String())
}

func operatorEq(left any, right any) any {
	return reflect.DeepEqual(left, right)
}
