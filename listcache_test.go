package rimcu

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListCache(t *testing.T) {
	lc, err := newListCache(100)
	require.NoError(t, err)
	require.NotNil(t, lc)

	const (
		key1 = "key1"
		val1 = "val1"
		val2 = "val2"
	)

	val, ok := lc.Lpop(key1)
	require.False(t, ok)
	require.Empty(t, val)

	lc.Rpush(key1, val1)
	lc.Rpush(key1, val2)

	val, ok = lc.Lpop(key1)
	require.True(t, ok)
	require.Equal(t, val1, val)

	val, ok = lc.Lpop(key1)
	require.True(t, ok)
	require.Equal(t, val2, val)

	val, ok = lc.Lpop(key1)
	require.False(t, ok)
	require.Empty(t, val)

}
