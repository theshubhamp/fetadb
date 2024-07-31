package encoding

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPositiveIntegerRoundTrip(t *testing.T) {
	encoded, err := Encode(int8(13))
	require.Nil(t, err)
	decoded := Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(13), decoded)

	encoded, err = Encode(int16(13))
	require.Nil(t, err)
	decoded = Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(13), decoded)

	encoded, err = Encode(int32(13))
	require.Nil(t, err)
	decoded = Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(13), decoded)

	encoded, err = Encode(int64(13))
	require.Nil(t, err)
	decoded = Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(13), decoded)
}

func TestNegativeIntegerRoundTrip(t *testing.T) {
	encoded, err := Encode(int8(-13))
	require.Nil(t, err)
	decoded := Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(-13), decoded)

	encoded, err = Encode(int16(-13))
	require.Nil(t, err)
	decoded = Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(-13), decoded)

	encoded, err = Encode(int32(-13))
	require.Nil(t, err)
	decoded = Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(-13), decoded)

	encoded, err = Encode(int64(-13))
	require.Nil(t, err)
	decoded = Decode(encoded)
	require.NotNil(t, decoded)
	require.Equal(t, int64(-13), decoded)
}
