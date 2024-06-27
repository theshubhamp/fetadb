package expr

import (
	"fmt"
	"reflect"
)

var methods = map[string]reflect.Value{
	"lower": reflect.ValueOf(Lower),
	"upper": reflect.ValueOf(Upper),
}

func NewFuncCall(name string, args []Expression) (FuncCall, error) {
	delegate, ok := methods[name]
	if !ok {
		return FuncCall{}, fmt.Errorf("function %v not found", name)
	}

	if len(args) != delegate.Type().NumIn() {
		return FuncCall{}, fmt.Errorf("function %v requires %v args, got %v", name, delegate.Type().NumIn(), len(args))
	}

	return FuncCall{Name: name, delegate: delegate, Args: args}, nil
}

type FuncCall struct {
	Name     string
	delegate reflect.Value
	Args     []Expression
}

func (f FuncCall) Evaluate(ec EvaluationContext) (any, error) {
	evaluatedArgs := []reflect.Value{}

	for _, arg := range f.Args {
		evaluatedArg, err := arg.Evaluate(ec)
		if err != nil {
			return nil, err
		}

		evaluatedArgs = append(evaluatedArgs, reflect.ValueOf(evaluatedArg))
	}

	return f.delegate.Call(evaluatedArgs)[0].Interface(), nil
}

func (f FuncCall) String() string {
	panic("implement me")
}
