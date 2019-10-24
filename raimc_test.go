package rimcu

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	// set pool
	pool1 := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", server.Addr())
		},
	}

	pool2 := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", server.Addr())
		},
	}

	// set client
	cli1, err := NewWithPool(Config{
		CacheSize: 10000,
	}, pool1)
	require.NoError(t, err)

	cli2, err := NewWithPool(Config{
		CacheSize: 10000,
	}, pool2)
	require.NoError(t, err)

	const (
		key1      = "key1"
		val1      = "val1"
		val2      = "val2"
		expSecond = 100
	)
	var (
		ctx = context.Background()
	)
	// - set from client1
	err = cli1.SetEx(ctx, key1, val1, expSecond)
	require.NoError(t, err)

	// -- check in cli2
	//  check it is the same in client2
	val, err := cli2.Get(ctx, key1, expSecond)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	//  check it is in mem of cli2
	val, ok := cli2.getMemCache(key1)
	require.True(t, ok)
	require.Equal(t, val1, val)

	// -- change from cli1 and see it is not mem of cli2 anymore
	// (a) change from cli1
	err = cli1.SetEx(ctx, key1, val2, expSecond)
	require.NoError(t, err)

	// wait a bit
	// TODO: find some better way
	time.Sleep(1 * time.Second)

	// (b) make sure it is not in memory of cli2 anymore
	val, ok = cli2.getMemCache(key1)
	require.False(t, ok)
	require.Empty(t, val)

	// (c) Get will give correct result
	val, err = cli2.Get(ctx, key1, expSecond)
	require.NoError(t, err)
	require.Equal(t, val2, val)

	// -------- delete test
	// del from c2
	err = cli2.Del(ctx, key1)
	require.NoError(t, err)

	// check in memory of c2
	val, ok = cli2.getMemCache(key1)
	require.False(t, ok)
	require.Empty(t, val)

	// TODO: find some better way
	time.Sleep(1 * time.Second)
	// check in c1
	val, err = cli1.Get(ctx, key1, expSecond)
	require.Equal(t, ErrNotFound, err)
	require.Empty(t, val)

}
