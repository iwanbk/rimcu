package resp2

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

// Test to make sure that push operation will not initiate in mem cache of:
// - the pusher
// - other clients which doesn't listen to the keys
func TestListCache_PushNotSetInMem(t *testing.T) {
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
func TestListCache_PushPropagate(t *testing.T) {
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

// Test that list pop will not initiate in-mem cache
func TestListCache_PopNotSetInMem(t *testing.T) {
	lcs, cleanup := createListCacheResp2TestClient(t, 2)
	defer cleanup()

	lc1, lc2 := lcs[0], lcs[1]

	var (
		key1 = xid.New().String()
		val1 = "val_1"
		val2 = "val_2"
		ctx  = context.Background()
	)

	// test initialization:fill the data
	{
		err := lc1.Rpush(ctx, key1, val1)
		require.NoError(t, err)

		err = lc2.Rpush(ctx, key1, val2)
		require.NoError(t, err)
	}

	// make sure initial condition
	{
		_, ok := lc1.memGet(key1)
		require.False(t, ok)
	}

	// do the action
	{
		_, _, err := lc2.Lpop(ctx, key1)
		require.NoError(t, err)

		_, _, err = lc1.Lpop(ctx, key1)
		require.NoError(t, err)

	}

	// check expected state

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
func TestListCache_PopPropagate(t *testing.T) {
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

	// test initialization
	{
		// - GET to listen to the RPUSH/LPOP event
		{
			_, err := lc1.Get(ctx, key1)
			require.NoError(t, err)

			_, err = lc2.Get(ctx, key1)
			require.NoError(t, err)
		}
		// PUSH some data
		{
			err := lc1.Rpush(ctx, key1, val1)
			require.NoError(t, err)

			err = lc1.Rpush(ctx, key1, val2)
			require.NoError(t, err)

			err = lc1.Rpush(ctx, key1, val3)
			require.NoError(t, err)
		}
	}

	// check initial condition:
	// lc1 must have the values in the memory
	{
		time.Sleep(listWaitDur)

		expected := []string{val1, val2, val3}

		cv, ok := lc1.memGet(key1)
		require.True(t, ok)
		require.Equal(t, expected, cv.GetList())
	}

	// do the action
	{
		val, ok, err := lc2.Lpop(ctx, key1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, val1, val)
	}

	// check expected result: it is properly propagated
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
func createListCacheResp2TestClient(t *testing.T, numCli int) ([]*ListCache, func()) {
	var (
		caches     []*ListCache
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
		cli, err := NewListCache(pool, ListCacheConfig{
			Logger:   &debugLogger{},
			ClientID: []byte(fmt.Sprintf("client_%d", i+1)),
			opIDGen:  &testOpIDGen{},
		})

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
