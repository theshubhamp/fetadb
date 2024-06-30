package expr

import (
	"fetadb/pkg/util"
	"fmt"
	"math"
	"reflect"
	"strings"
)

var functions = map[string]reflect.Value{
	"=":     reflect.ValueOf(Eq),
	"+":     reflect.ValueOf(Add),
	"-":     reflect.ValueOf(Subtract),
	"*":     reflect.ValueOf(Multiply),
	"/":     reflect.ValueOf(Divide),
	"lower": reflect.ValueOf(Lower),
	"upper": reflect.ValueOf(Upper),
	"||":    reflect.ValueOf(Concat),
	"md5":   reflect.ValueOf(Md5),
}

func NewFuncCall(name string, args []Expression) (FuncCall, error) {
	delegate, ok := functions[name]
	if !ok {
		return FuncCall{}, fmt.Errorf("function %v not found", name)
	}

	if len(args) != delegate.Type().NumIn() {
		return FuncCall{}, fmt.Errorf("function %v requires %v args, got %v", name, delegate.Type().NumIn(), len(args))
	}

	if delegate.Type().NumOut() == 0 {
		return FuncCall{}, fmt.Errorf("function %v requires at least 1 return, got 0", name)
	}

	resultOut := -1
	errorOut := -1
	for idx := range delegate.Type().NumOut() {
		if util.IsError(delegate.Type().Out(idx)) {
			errorOut = idx
		} else {
			resultOut = int(math.Max(float64(resultOut), float64(idx)))
		}
	}

	return FuncCall{Name: name, delegate: delegate, resultOut: resultOut, errorOut: errorOut, Args: args}, nil
}

type FuncCall struct {
	Name      string
	delegate  reflect.Value
	resultOut int
	errorOut  int
	Args      []Expression
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

	returns := f.delegate.Call(evaluatedArgs)
	var val any = nil
	var err error = nil

	if f.resultOut >= 0 {
		val = returns[f.resultOut].Interface()
	}
	if f.errorOut >= 0 {
		errRet := returns[f.errorOut].Interface()
		if errRet != nil {
			err = errRet.(error)
		}
	}

	return val, err
}

func (f FuncCall) String() string {
	args := []string{}
	for _, arg := range f.Args {
		args = append(args, arg.String())
	}

	return fmt.Sprintf("%v(%v)", f.Name, strings.Join(args, ","))
}
