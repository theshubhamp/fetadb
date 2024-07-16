package expr

import (
	"github.com/stretchr/testify/require"
	"testing"
)

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
