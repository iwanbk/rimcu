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

	scr1 := NewStringsCache(StringsCacheConfig{
		ServerAddr: serverAddr,
		CacheSize:  cacheSize,
	})

	scr2 := NewStringsCache(StringsCacheConfig{
		ServerAddr: serverAddr,
		CacheSize:  cacheSize,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// set to val1
	err := scr1.Setex(ctx, key1, val1, exp)
	require.NoError(t, err)

	// get from scr2, will also populate that value in scr2 in mem cache
	{
		val, err := scr2.Get(ctx, key1, exp)
		require.NoError(t, err)
		require.Equal(t, val1, val)

		val, ok := scr2.memGet(key1)
		require.True(t, ok)
		require.Equal(t, val1, val)
	}

	// when the cache changed from scr1, scr2 in mem cache must be deleted
	{
		// change to val2
		err = scr1.Setex(ctx, key1, val2, exp)
		require.NoError(t, err)

		time.Sleep(3 * time.Second) // TODO: find better way to wait

		// it should be deleted from scr2 inmem cache
		// and the call to memGet must failed
		_, ok := scr2.memGet(key1)
		require.False(t, ok)
	}

	//--- delete from scr2, it should disappeared from scr1
	{
		// populate in mem cache of scr1
		_, err = scr1.Get(ctx, key1, exp)
		require.NoError(t, err)

		_, ok := scr1.memGet(key1)
		require.True(t, ok)

		// delete from scr2
		err = scr2.Del(ctx, key1)
		require.NoError(t, err)

		// should be disapperad from scr1
		time.Sleep(3 * time.Second) // TODO: find better way to wait

		_, ok = scr1.memGet(key1)
		require.False(t, ok)
	}
}
