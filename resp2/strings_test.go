package resp2

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

var (
	testExpSecond = 1000
	syncTimeWait  = 1 * time.Second
)

// Test Set WILL initiate in memory cache
func TestStringsCacheResp2_Set_InitInMem(t *testing.T) {
	clis, cleanup := createStringsResp2TestClient(t, 2)
	defer cleanup()

	var (
		cli1      = clis[0]
		key1      = generateRandomKey()
		val1      = "val1"
		expSecond = 100
		ctx       = context.Background()
	)

	// Test Init
	{

	}

	// make sure initial condition
	{
		val, ok := cli1.getMemCache(key1)
		require.False(t, ok)
		require.Empty(t, val)
	}

	// do action
	{
		// - set from client1
		err := cli1.Setex(ctx, key1, val1, expSecond)
		require.NoError(t, err)

	}

	// check expected condition
	{
		val, ok := cli1.getMemCache(key1)
		require.True(t, ok)
		require.Equal(t, val1, val)
	}

}

// Test that Set will invalidate memcache in other nodes
func TestStringsCacheResp2_Set_Invalidate(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsResp2TestClient(t, 3)
	defer cleanup()

	var (
		sc1, sc2, sc3 = scs[0], scs[1], scs[2]
		key1          = generateRandomKey()
		val1          = "val_1"
		val2          = "val_2"
	)

	{ // Test initialization, get the value to activate listening
		// Set
		err := sc1.Setex(ctx, key1, val1, testExpSecond)
		require.NoError(t, err)

		// Get to activate listening
		_, err = sc2.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)

		_, err = sc3.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)
	}

	// make sure initial condition, key1 must exists in memcache
	{
		_, ok := sc2.getMemCache(key1)
		require.True(t, ok)

		_, ok = sc3.getMemCache(key1)
		require.True(t, ok)
	}

	// do the action : Set
	{
		// set
		err := sc1.Setex(ctx, key1, val2, testExpSecond)
		require.NoError(t, err)
	}
	time.Sleep(syncTimeWait)

	// check expected condition
	{

		// check
		_, ok := sc2.getMemCache(key1)
		require.False(t, ok)

		_, ok = sc3.getMemCache(key1)
		require.False(t, ok)
	}

}

// Get valid key must initiate inmem cache
func TestStringsCacheResp2_Get_Valid_InitInMem(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsResp2TestClient(t, 3)
	defer cleanup()

	var (
		sc1, sc2, sc3 = scs[0], scs[1], scs[2]
		key1          = generateRandomKey()
		val1          = "val_1"
	)

	// Test initialization: set the key in redis
	{
		// set
		err := sc1.Setex(ctx, key1, val1, testExpSecond)
		require.NoError(t, err)

		val, err := sc1.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)
		require.Equal(t, val1, val)
	}

	// make sure initial condition: key not exist in memcache
	{
		_, ok := sc2.getMemCache(key1)
		require.False(t, ok)

		_, ok = sc3.getMemCache(key1)
		require.False(t, ok)
	}

	// do the action : Get
	{
		// get
		val, err := sc2.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)
		require.Equal(t, val1, val)

		val, err = sc3.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)
		require.Equal(t, val1, val)
	}

	// check expected condition
	// key1 exists in memcache
	{

		// check
		_, ok := sc2.getMemCache(key1)
		require.True(t, ok)

		_, ok = sc3.getMemCache(key1)
		require.True(t, ok)
	}

}

// Get invalid key must:
// -  not initiate in mem cache
// - got error
func TestStringsCacheResp2_Get_Invalid_NotInitInMem(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsResp2TestClient(t, 3)
	defer cleanup()

	var (
		sc1  = scs[0]
		key1 = generateRandomKey()
	)

	// make sure initial condition, key1 must not exists in memcache
	{
		_, ok := sc1.getMemCache(key1)
		require.False(t, ok)
	}

	// do the action : get key that not exists
	{
		// get
		_, err := sc1.Get(ctx, key1, testExpSecond)
		require.Error(t, err)
	}

	// check expected condition
	// key1 not exists in memcache
	{

		// check
		_, ok := sc1.getMemCache(key1)
		require.False(t, ok)

	}

}

// Test Del : deleting valid key must propagate to other nodes
func TestStringsCacheResp2_Del_ValidKey_Propagate(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsResp2TestClient(t, 3)
	defer cleanup()

	var (
		sc1, sc2, sc3 = scs[0], scs[1], scs[2]
		key1          = generateRandomKey()
		val1          = "val_1"
	)

	{ // Test initialization, get the value to activate listening
		// Set
		err := sc1.Setex(ctx, key1, val1, testExpSecond)
		require.NoError(t, err)

		// Get to activate listening
		_, err = sc2.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)

		_, err = sc3.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)
	}

	// make sure initial condition, key1 must exists in memcache
	{
		_, ok := sc2.getMemCache(key1)
		require.True(t, ok)

		_, ok = sc3.getMemCache(key1)
		require.True(t, ok)
	}

	// do the action : Del
	{
		// set
		err := sc1.Del(ctx, key1)
		require.NoError(t, err)
	}

	time.Sleep(syncTimeWait)

	// check expected condition
	{

		_, ok := sc1.getMemCache(key1)
		require.False(t, ok)

		// check
		_, ok = sc2.getMemCache(key1)
		require.False(t, ok)

		_, ok = sc3.getMemCache(key1)
		require.False(t, ok)
	}
}

func createStringsResp2TestClient(t *testing.T, numCli int) ([]*StringsCache, func()) {
	var (
		caches     []*StringsCache
		serverAddr = os.Getenv("TEST_REDIS_ADDR")
		server     *miniredis.Miniredis
	)

	if serverAddr == "" { // if we don't have redis server for tests
		server, err := miniredis.Run()
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
		cli, err := NewStringsCacheResp2(StringsCacheResp2Config{
			CacheSize: 10000,
			Logger:    &debugLogger{},
			ClientID:  []byte(fmt.Sprintf("client_%d", i+1)),
		}, pool)
		require.NoError(t, err)
		caches = append(caches, cli)
	}

	return caches, func() {
		if server != nil {
			server.Close()
		}
		for _, cli := range caches {
			cli.Close()
		}
	}
}

func generateRandomKey() string {
	return xid.New().String()
}

type debugLogger struct {
}

func (d *debugLogger) Debugf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (d *debugLogger) Errorf(format string, v ...interface{}) {
	log.Printf(format, v...)
}
