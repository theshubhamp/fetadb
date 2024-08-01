package expr

import (
	"fmt"
	"strings"
)

func NewAndOperator(args []Expression) (And, error) {
	delegate, err := NewFuncCall("and", args)
	if err != nil {
		return And{}, err
	}

	return And{
		delegate:   delegate,
		Conditions: args,
	}, nil
}

type And struct {
	delegate   FuncCall
	Conditions []Expression
}

func (a And) Evaluate(ec EvaluationContext) (any, error) {
	return a.delegate.Evaluate(ec)
}

func (a And) String() string {
	conds := []string{}
	for _, cond := range a.Conditions {
		conds = append(conds, cond.String())
	}

	return fmt.Sprintf(strings.Join(conds, " and "))
}

func NewOrOperator(args []Expression) (Or, error) {
	delegate, err := NewFuncCall("or", args)
	if err != nil {
		return Or{}, err
	}

	return Or{
		delegate:   delegate,
		Conditions: args,
	}, nil
}

type Or struct {
	delegate   FuncCall
	Conditions []Expression
}

func (a Or) Evaluate(ec EvaluationContext) (any, error) {
	return a.delegate.Evaluate(ec)
}

func (a Or) String() string {
	conds := []string{}
	for _, cond := range a.Conditions {
		conds = append(conds, cond.String())
	}

	return fmt.Sprintf(strings.Join(conds, " or "))
}
