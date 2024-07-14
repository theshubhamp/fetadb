package expr

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestHasFunc(t *testing.T) {
	require.False(t, HasFunc("should_not_be_found"))
}

func TestFunctionCall(t *testing.T) {
	testString := "ABCD"

	funcCall, err := NewFuncCall("lower", []Expression{Literal{Value: testString}})
	require.Nil(t, err)

	result, err := funcCall.Evaluate(nil)
	require.Nil(t, err)
	require.IsType(t, "", result)
	require.Equal(t, strings.ToLower(testString), result)
}
