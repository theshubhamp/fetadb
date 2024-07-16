package expr

import (
	"fetadb/pkg/util/types/dataframe"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

type CurrentRowContext struct {
	Cols map[string]any
}

func (c *CurrentRowContext) LookupColumnRef(ref ColumnRef) (any, error) {
	val, ok := c.Cols[ref.String()]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	return val, nil
}

func TestHasAgg(t *testing.T) {
	require.False(t, HasAgg("should_not_be_found"))
}

func TestAggCount(t *testing.T) {
	aggCall, err := NewAggCall("count", []Expression{})
	require.Nil(t, err)

	require.Equal(t, int64(0), aggCall.Instance.Agg.Aggregate())

	currValue, err := aggCall.Evaluate(nil)
	require.Nil(t, err)
	require.Equal(t, int64(1), currValue)

	aggCall.Instance.Agg.Reset()
	require.Equal(t, int64(0), aggCall.Instance.Agg.Aggregate())
}

func TestAggMax(t *testing.T) {
	aggCall, err := NewAggCall("max", []Expression{ColumnRef(dataframe.NewColumnRef("column"))})
	require.Nil(t, err)

	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())

	currValue, err := aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 1}})
	require.Nil(t, err)
	require.Equal(t, float64(1), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 2}})
	require.Nil(t, err)
	require.Equal(t, float64(2), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 3}})
	require.Nil(t, err)
	require.Equal(t, float64(3), currValue)

	aggCall.Instance.Agg.Reset()
	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())
}

func TestAggMin(t *testing.T) {
	aggCall, err := NewAggCall("min", []Expression{ColumnRef(dataframe.NewColumnRef("column"))})
	require.Nil(t, err)

	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())

	currValue, err := aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 3}})
	require.Nil(t, err)
	require.Equal(t, float64(3), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 2}})
	require.Nil(t, err)
	require.Equal(t, float64(2), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 1}})
	require.Nil(t, err)
	require.Equal(t, float64(1), currValue)

	aggCall.Instance.Agg.Reset()
	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())
}

func TestAggSum(t *testing.T) {
	aggCall, err := NewAggCall("sum", []Expression{ColumnRef(dataframe.NewColumnRef("column"))})
	require.Nil(t, err)

	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())

	currValue, err := aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 3}})
	require.Nil(t, err)
	require.Equal(t, float64(3), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 2}})
	require.Nil(t, err)
	require.Equal(t, float64(5), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 1}})
	require.Nil(t, err)
	require.Equal(t, float64(6), currValue)

	aggCall.Instance.Agg.Reset()
	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())
}

func TestAggAvg(t *testing.T) {
	aggCall, err := NewAggCall("avg", []Expression{ColumnRef(dataframe.NewColumnRef("column"))})
	require.Nil(t, err)

	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())

	currValue, err := aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 3}})
	require.Nil(t, err)
	require.Equal(t, float64(3), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 2}})
	require.Nil(t, err)
	require.Equal(t, float64(2.5), currValue)

	currValue, err = aggCall.Evaluate(&CurrentRowContext{Cols: map[string]any{"column": 1}})
	require.Nil(t, err)
	require.Equal(t, float64(2), currValue)

	aggCall.Instance.Agg.Reset()
	require.Equal(t, float64(0), aggCall.Instance.Agg.Aggregate())
}
