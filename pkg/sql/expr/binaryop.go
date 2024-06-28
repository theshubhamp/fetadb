package expr

import "fmt"

func NewBinaryOperator(operator string, left Expression, right Expression) (BinaryOperator, error) {
	delegate, err := NewFuncCall(operator, []Expression{left, right})
	if err != nil {
		return BinaryOperator{}, err
	}

	return BinaryOperator{
		Operator: operator,
		delegate: delegate,
		Left:     left,
		Right:    right,
	}, nil
}

type BinaryOperator struct {
	Operator string
	delegate FuncCall
	Left     Expression
	Right    Expression
}

func (b BinaryOperator) Evaluate(ec EvaluationContext) (any, error) {
	return b.delegate.Evaluate(ec)
}

func (b BinaryOperator) String() string {
	return fmt.Sprintf("%v %v %v", b.Left.String(), b.Operator, b.Right.String())
}
