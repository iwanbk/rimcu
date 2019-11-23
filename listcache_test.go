package rimcu

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

var (
	listWaitDur = 2 * time.Second
)

// Test to make sure that push operation will not initiate in mem cache
func TestListCachePushNotSetInMem(t *testing.T) {
	lcs, cleanup := createListCacheResp2TestClient(t, 2)
	defer cleanup()

	lc1, lc2 := lcs[0], lcs[1]

	var (
		key1 = xid.New().String()
		val1 = "val_1"
		val2 = "val_2"
		val3 = "val_3"
		ctx  = context.Background()
	)

	err := lc1.Rpush(ctx, key1, val1)
	require.NoError(t, err)

	err = lc2.Rpush(ctx, key1, val2)
	require.NoError(t, err)

	err = lc1.Rpush(ctx, key1, val3)
	require.NoError(t, err)

	{
		time.Sleep(listWaitDur)

		cv, ok := lc1.memGet(key1)
		require.False(t, ok)
		require.Nil(t, cv)

		_, ok = lc2.memGet(key1)
		require.False(t, ok)
		require.Nil(t, cv)
	}

}

// Test RPUSH propagates to other node
func TestListCachePushPropagate(t *testing.T) {
	lcs, cleanup := createListCacheResp2TestClient(t, 2)
	defer cleanup()

	lc1, lc2 := lcs[0], lcs[1]

	var (
		key1 = xid.New().String()
		val1 = "val_1"
		val2 = "val_2"
		val3 = "val_3"
		ctx  = context.Background()
	)

	// GET to listen to the RPUSH event
	{
		val, err := lc1.Get(ctx, key1)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.Empty(t, val)

		val, err = lc2.Get(ctx, key1)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.Empty(t, val)
	}
	// PUSH
	{
		err := lc1.Rpush(ctx, key1, val1)
		require.NoError(t, err)

		err = lc2.Rpush(ctx, key1, val2)
		require.NoError(t, err)

		err = lc1.Rpush(ctx, key1, val3)
		require.NoError(t, err)
	}

	// check it is properly propagated
	{
		time.Sleep(listWaitDur)

		expected := []string{val1, val2, val3}

		cv, ok := lc1.memGet(key1)
		require.True(t, ok)
		require.Equal(t, expected, cv.GetList())

		cv, ok = lc2.memGet(key1)
		require.True(t, ok)
		require.Equal(t, expected, cv.GetList())
	}

}

func TestListCachePopNotSetInMem(t *testing.T) {
	lcs, cleanup := createListCacheResp2TestClient(t, 2)
	defer cleanup()

	lc1, lc2 := lcs[0], lcs[1]

	var (
		key1 = xid.New().String()
		val1 = "val_1"
		val2 = "val_2"
		ctx  = context.Background()
	)

	err := lc1.Rpush(ctx, key1, val1)
	require.NoError(t, err)

	err = lc2.Rpush(ctx, key1, val2)
	require.NoError(t, err)

	_, _, err = lc2.Lpop(ctx, key1)
	require.NoError(t, err)

	_, _, err = lc1.Lpop(ctx, key1)
	require.NoError(t, err)

	{
		time.Sleep(listWaitDur)

		cv, ok := lc1.memGet(key1)
		require.False(t, ok)
		require.Nil(t, cv)

		_, ok = lc2.memGet(key1)
		require.False(t, ok)
		require.Nil(t, cv)
	}

}

// Test LPOP propagates to other node
func TestListCachePopPropagate(t *testing.T) {
	lcs, cleanup := createListCacheResp2TestClient(t, 2)
	defer cleanup()

	lc1, lc2 := lcs[0], lcs[1]

	var (
		key1 = xid.New().String()
		val1 = "val_1"
		val2 = "val_2"
		val3 = "val_3"
		ctx  = context.Background()
	)

	// GET to listen to the RPUSH/LPOP event
	{
		_, err := lc1.Get(ctx, key1)
		require.NoError(t, err)

		_, err = lc2.Get(ctx, key1)
		require.NoError(t, err)
	}
	// PUSH
	{
		err := lc1.Rpush(ctx, key1, val1)
		require.NoError(t, err)

		err = lc1.Rpush(ctx, key1, val2)
		require.NoError(t, err)

		err = lc1.Rpush(ctx, key1, val3)
		require.NoError(t, err)
	}

	{ // POP
		val, ok, err := lc2.Lpop(ctx, key1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, val1, val)
	}

	// check it is properly propagated
	{
		time.Sleep(listWaitDur)

		expected := []string{val2, val3}

		cv, ok := lc1.memGet(key1)
		require.True(t, ok)
		require.Equal(t, expected, cv.GetList())

		cv, ok = lc2.memGet(key1)
		require.True(t, ok)
		require.Equal(t, expected, cv.GetList())
	}

}
func createListCacheResp2TestClient(t *testing.T, numCli int) ([]*listCache, func()) {
	var (
		caches     []*listCache
		serverAddr = os.Getenv("TEST_REDIS_ADDR")
		server     *miniredis.Miniredis
		err        error
	)

	if serverAddr == "" { // if we don't have redis server for tests
		server, err = miniredis.Run()
		require.NoError(t, err)

		time.Sleep(1 * time.Second) // miniredis sometime need startup time

		serverAddr = server.Addr()
	}

	// set pool
	for i := 0; i < numCli; i++ {
		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", serverAddr)
			},
		}
		cli, err := newListCache(pool, 1000, []byte(fmt.Sprintf("client_%d", i)))
		require.NoError(t, err)
		caches = append(caches, cli)
	}

	return caches, func() {
		if server != nil {
			server.Close()
		}
		//for _, cli := range caches {
		//cli.Close()
		//}
	}
}
