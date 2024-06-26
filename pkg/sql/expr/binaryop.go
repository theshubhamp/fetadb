package expr

import (
	"fmt"
	"reflect"
)

type delegateFunc func(left any, right any) (any, error)

var delegates = map[string]delegateFunc{
	"=": operatorEq,
	"+": Add,
	"-": Subtract,
	"*": Multiply,
	"/": Divide,
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

func (b BinaryOperator) Evaluate(ec EvaluationContext) (any, error) {
	leftResult, err := b.Left.Evaluate(ec)
	if err != nil {
		return nil, err
	}
	rightResult, err := b.Right.Evaluate(ec)
	if err != nil {
		return nil, err
	}

	return b.delegate(leftResult, rightResult)
}

func (b BinaryOperator) String() string {
	return fmt.Sprintf("%v %v %v", b.Left.String(), b.Operator, b.Right.String())
}

func operatorEq(left any, right any) (any, error) {
	return reflect.DeepEqual(left, right), nil
}
