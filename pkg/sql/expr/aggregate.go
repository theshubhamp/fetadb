package expr

import (
	"fetadb/pkg/util/types"
	"fmt"
	"math"
	"reflect"
	"strings"
)

type Agg interface {
	Aggregate() any
	Reset()
}

type AggInstance struct {
	Delegate reflect.Value
	Agg      Agg
}

func (a *AggInstance) Consume(args []reflect.Value) {
	a.Delegate.Call(args)
}

var aggregates = map[string]func() AggInstance{
	"count": func() AggInstance {
		agg := count{}
		return AggInstance{Delegate: reflect.ValueOf(agg.Consume), Agg: &agg}
	},
	"min": func() AggInstance {
		agg := minimum{zero: true, min: 0}
		return AggInstance{Delegate: reflect.ValueOf(agg.Consume), Agg: &agg}
	},
	"max": func() AggInstance {
		agg := maximum{zero: true, max: 0}
		return AggInstance{Delegate: reflect.ValueOf(agg.Consume), Agg: &agg}
	},
}

func HasAgg(name string) bool {
	_, ok := aggregates[name]
	return ok
}

func NewAggCall(name string, args []Expression) (AggCall, error) {
	creator, ok := aggregates[name]
	if !ok {
		return AggCall{}, fmt.Errorf("agg %v not found", name)
	}
	agg := creator()

	if len(args) != agg.Delegate.Type().NumIn() {
		return AggCall{}, fmt.Errorf("agg %v requires %v args, got %v", name, agg.Delegate.Type().NumIn(), len(args))
	}

	return AggCall{Name: name, Args: args, Instance: agg}, nil
}

type AggCall struct {
	Name     string
	Args     []Expression
	Instance AggInstance
}

func (a AggCall) Evaluate(ec EvaluationContext) (any, error) {
	evaluatedArgs := []reflect.Value{}

	for _, arg := range a.Args {
		evaluatedArg, err := arg.Evaluate(ec)
		if err != nil {
			return nil, err
		}

		if evaluatedArg == nil {
			evaluatedArg = types.Null
		}

		evaluatedArgs = append(evaluatedArgs, reflect.ValueOf(evaluatedArg))
	}

	a.Instance.Consume(evaluatedArgs)

	return a.Instance.Agg.Aggregate(), nil
}

func (a AggCall) String() string {
	args := []string{}
	for _, arg := range a.Args {
		args = append(args, arg.String())
	}

	return fmt.Sprintf("%v(%v)", a.Name, strings.Join(args, ","))
}

type count struct {
	Items int64
}

func (c *count) Consume() {
	c.Items++
}

func (c *count) Aggregate() any {
	return c.Items
}

func (c *count) Reset() {
	c.Items = 0
}

type minimum struct {
	min  float64
	zero bool
}

func (m *minimum) Consume(val any) {
	num, _ := types.NewNumber(val)

	if m.zero {
		m.min = num.Float()
		m.zero = false
	} else {
		m.min = math.Min(m.min, num.Float())
	}
}

func (m *minimum) Aggregate() any {
	return m.min
}

func (m *minimum) Reset() {
	m.min = 0
	m.zero = true
}

type maximum struct {
	max  float64
	zero bool
}

func (m *maximum) Consume(val any) {
	num, _ := types.NewNumber(val)

	if m.zero {
		m.max = num.Float()
		m.zero = false
	} else {
		m.max = math.Max(m.max, num.Float())
	}
}

func (m *maximum) Aggregate() any {
	return m.max
}

func (m *maximum) Reset() {
	m.max = 0
	m.zero = true
}
