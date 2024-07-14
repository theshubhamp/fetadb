package dataframe

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestArrayOrdered(t *testing.T) {
	array := NewArrayOrdered[uint64]()
	require.NotNil(t, array.New())
	require.Equal(t, 0, array.Length())

	array.Append(uint64(1))
	array.Append(uint64(2))
	require.Equal(t, 2, array.Length())
	require.True(t, array.Less(0, 1))
	require.False(t, array.Equals(0, 1))

	array.Swap(0, 1)
	require.False(t, array.Less(0, 1))
	require.Equal(t, uint64(2), array.Get(0))
	require.Equal(t, uint64(1), array.Get(1))

	array.Append(nil)
	require.Equal(t, 3, array.Length())
	require.Nil(t, array.Get(2))
}

func TestArrayBool(t *testing.T) {
	array := NewArrayBool()
	require.NotNil(t, array.New())
	require.Equal(t, 0, array.Length())

	array.Append(false)
	array.Append(true)
	require.Equal(t, 2, array.Length())
	require.True(t, array.Less(0, 1))
	require.False(t, array.Equals(0, 1))

	array.Swap(0, 1)
	require.False(t, array.Less(0, 1))
	require.Equal(t, true, array.Get(0))
	require.Equal(t, false, array.Get(1))

	array.Append(nil)
	require.Equal(t, 3, array.Length())
	require.Nil(t, array.Get(2))
}
