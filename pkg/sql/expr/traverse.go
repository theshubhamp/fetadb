package expr

type ReduceCallback[T any] func(accum T, next Expression) T

func Reduce[T any](expression Expression, callback ReduceCallback[T], initialValue T) T {
	lastValue := initialValue
	queue := []Expression{expression}
	for len(queue) > 0 {
		top := queue[0]
		queue = queue[1:]

		lastValue = callback(lastValue, top)

		switch expr := top.(type) {
		case AggCall:
			for _, arg := range expr.Args {
				queue = append(queue, arg)
			}
		case FuncCall:
			for _, arg := range expr.Args {
				queue = append(queue, arg)
			}
		case BinaryOperator:
			queue = append(queue, expr.Left, expr.Right)
		case ColumnRef:
		case Literal:
		default:
		}
	}

	return lastValue
}

type IterCallback func(expression Expression)

func Iter(expression Expression, callback IterCallback) {
	Reduce[any](expression, func(accum any, next Expression) any {
		callback(next)
		return nil
	}, nil)
}

func ContainsAgg(expression Expression) bool {
	return Reduce[bool](expression, func(accum bool, next Expression) bool {
		_, agg := next.(AggCall)
		return accum || agg
	}, false)
}
