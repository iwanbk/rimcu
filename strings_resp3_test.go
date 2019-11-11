package rimcu

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStringsCacheResp3(t *testing.T) {
	const (
		serverAddr = "localhost:6379"
		cacheSize  = 100
		key1       = "key_1"
		val1       = "TestStringsCacheResp3_val1"
		val2       = "TestStringsCacheResp3_val2"
		exp        = 100
	)

	scr1 := NewStringsCacheResp3(StringsCacheResp3Config{
		ServerAddr: serverAddr,
		CacheSize:  cacheSize,
	})

	scr2 := NewStringsCacheResp3(StringsCacheResp3Config{
		ServerAddr: serverAddr,
		CacheSize:  cacheSize,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// set to val1
	err := scr1.Setex(ctx, key1, val1, exp)
	require.NoError(t, err)

	val, err := scr2.Get(ctx, key1, exp)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	val, ok := scr2.memGet(key1)
	require.True(t, ok)
	require.Equal(t, val1, val)

	// change to val2
	err = scr1.Setex(ctx, key1, val2, exp)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// it should be deleted from scr2 inmem cache
	// and the call to memGet must failed
	val, ok = scr2.memGet(key1)
	require.False(t, ok)
}
