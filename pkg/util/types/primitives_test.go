package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNumber_Int(t *testing.T) {
	number, ok := NewNumber(int64(2))
	require.True(t, ok)

	require.True(t, number.IsInt())
	require.Equal(t, int64(2), number.Int())
	require.Equal(t, uint64(2), number.Uint())
	require.Equal(t, float64(2.0), number.Float())
}

func TestNumber_Uint(t *testing.T) {
	number, ok := NewNumber(uint64(2))
	require.True(t, ok)

	require.True(t, number.IsUint())
	require.Equal(t, int64(2), number.Int())
	require.Equal(t, uint64(2), number.Uint())
	require.Equal(t, float64(2.0), number.Float())
}

func TestNumber_Float(t *testing.T) {
	number, ok := NewNumber(float64(2.5))
	require.True(t, ok)

	require.True(t, number.IsFloat())
	require.Equal(t, int64(2), number.Int())
	require.Equal(t, uint64(2), number.Uint())
	require.Equal(t, float64(2.5), number.Float())
}

func TestNumberInvalid(t *testing.T) {
	_, ok := NewNumber("invalid")
	require.False(t, ok)
}
