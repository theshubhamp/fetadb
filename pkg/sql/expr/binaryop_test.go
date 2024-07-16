package expr

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewBinaryOperatorInvalid(t *testing.T) {
	_, err := NewBinaryOperator("++", Literal{Value: 1}, Literal{Value: 2})
	require.NotNil(t, err)
}

func TestBinaryOperator_String(t *testing.T) {
	operator, err := NewBinaryOperator("+", Literal{Value: 1}, Literal{Value: 2})
	require.Nil(t, err)

	repr := operator.String()
	require.Equal(t, "1 + 2", repr)
}

func TestBinaryOperatorPlus(t *testing.T) {
	operator, err := NewBinaryOperator("+", Literal{Value: 1}, Literal{Value: 2})
	require.Nil(t, err)

	result, err := operator.Evaluate(nil)
	require.Nil(t, err)
	require.Equal(t, int64(3), result)
}

func TestBinaryOperatorMinus(t *testing.T) {
	operator, err := NewBinaryOperator("-", Literal{Value: 1}, Literal{Value: 2})
	require.Nil(t, err)

	result, err := operator.Evaluate(nil)
	require.Nil(t, err)
	require.Equal(t, int64(-1), result)
}

func TestBinaryOperatorMultiply(t *testing.T) {
	operator, err := NewBinaryOperator("*", Literal{Value: 1}, Literal{Value: 2})
	require.Nil(t, err)

	result, err := operator.Evaluate(nil)
	require.Nil(t, err)
	require.Equal(t, int64(2), result)
}

func TestBinaryOperatorDivide(t *testing.T) {
	operator, err := NewBinaryOperator("/", Literal{Value: 1}, Literal{Value: 2})
	require.Nil(t, err)

	result, err := operator.Evaluate(nil)
	require.Nil(t, err)
	require.Equal(t, int64(0), result)
}
