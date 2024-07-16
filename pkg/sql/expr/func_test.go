package expr

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestHasFunc(t *testing.T) {
	require.False(t, HasFunc("should_not_be_found"))
}

func TestFuncCallLower(t *testing.T) {
	testString := "ABCD"

	funcCall, err := NewFuncCall("lower", []Expression{Literal{Value: testString}})
	require.Nil(t, err)

	result, err := funcCall.Evaluate(nil)
	require.Nil(t, err)
	require.IsType(t, "", result)
	require.Equal(t, strings.ToLower(testString), result)
}

func TestFuncCallUpper(t *testing.T) {
	testString := "abcd"

	funcCall, err := NewFuncCall("upper", []Expression{Literal{Value: testString}})
	require.Nil(t, err)

	result, err := funcCall.Evaluate(nil)
	require.Nil(t, err)
	require.IsType(t, "", result)
	require.Equal(t, strings.ToUpper(testString), result)
}

func TestFuncCallMd5(t *testing.T) {
	testString := "abcd"

	funcCall, err := NewFuncCall("md5", []Expression{Literal{Value: testString}})
	require.Nil(t, err)

	result, err := funcCall.Evaluate(nil)
	require.Nil(t, err)
	require.IsType(t, "", result)

	hash := md5.Sum([]byte(testString))
	require.Equal(t, hex.EncodeToString(hash[:]), result)
}

func TestFuncCallConcat(t *testing.T) {
	left := "abcd"
	right := "efgh"

	funcCall, err := NewFuncCall("||", []Expression{Literal{Value: left}, Literal{Value: right}})
	require.Nil(t, err)

	result, err := funcCall.Evaluate(nil)
	require.Nil(t, err)
	require.IsType(t, "", result)

	require.Equal(t, strings.Join([]string{left, right}, ""), result)
}
