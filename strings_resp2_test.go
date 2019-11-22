package rimcu

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestStringsCacheResp2SetGet(t *testing.T) {

	clis, cleanup := createStringsResp2TestClient(t, 2)
	defer cleanup()

	require.Len(t, clis, 2)

	cli1, cli2 := clis[0], clis[1]

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
	err := cli1.Setex(ctx, key1, val1, expSecond)
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
	err = cli1.Setex(ctx, key1, val2, expSecond)
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
}

func TestStringsCacheResp2Delete(t *testing.T) {
	clis, cleanup := createStringsResp2TestClient(t, 2)
	defer cleanup()

	require.Len(t, clis, 2)

	cli1, cli2 := clis[0], clis[1]

	const (
		key1      = "key1"
		val1      = "val1"
		val2      = "val2"
		expSecond = 100
	)
	var (
		ctx = context.Background()
	)

	// initialize data
	err := cli1.Setex(ctx, key1, val1, expSecond)
	require.NoError(t, err)

	val, err := cli2.Get(ctx, key1, expSecond)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	// del from c2
	err = cli2.Del(ctx, key1)
	require.NoError(t, err)

	// check in memory of c2, make sure not exists
	val, ok := cli2.getMemCache(key1)
	require.False(t, ok)
	require.Empty(t, val)

	// TODO: find some better way to wait
	time.Sleep(1 * time.Second)

	// check in  memory of c1, make sure not exists
	val, ok = cli1.getMemCache(key1)
	require.False(t, ok)
	require.Empty(t, val)

}

func createStringsResp2TestClient(t *testing.T, numCli int) ([]*StringsCacheResp2, func()) {
	var caches []*StringsCacheResp2

	server, err := miniredis.Run()
	require.NoError(t, err)

	serverAddr := server.Addr()

	// set pool
	for i := 0; i < numCli; i++ {

		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", serverAddr)
			},
		}
		cli, err := NewStringsCacheResp2(StringsCacheResp2Config{
			CacheSize: 10000,
			Logger:    &debugLogger{},
		}, pool)
		require.NoError(t, err)
		caches = append(caches, cli)
	}

	return caches, func() {
		server.Close()
		for _, cli := range caches {
			cli.Close()
		}
	}
}
