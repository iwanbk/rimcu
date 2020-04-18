package resp2

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/iwanbk/rimcu/internal/redigo/redis"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

var (
	testExpSecond = 1000
	syncTimeWait  = 2 * time.Second
)

// Test that Set will invalidate memcache in other nodes
func TestStringsCacheResp2_Set_Invalidate(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsResp2TestClient(t, 2)
	defer cleanup()

	var (
		sc1, sc2 = scs[0], scs[1]
		key1     = generateRandomKey()
		val1     = "val_1"
		val2     = "val_2"
	)

	time.Sleep(2 * time.Second)

	{ // Test initialization, get the value to activate listening
		// Set
		err := sc1.Setex(ctx, key1, val1, testExpSecond)
		require.NoError(t, err)

		// Get to activate listening
		_, err = sc2.Get(ctx, key1, testExpSecond)
		require.NoError(t, err)

	}

	// make sure initial condition, key1 must exists in memcache
	{
		_, ok := sc2.cc.Get(key1)
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
		_, ok := sc2.cc.Get(key1)
		require.False(t, ok)
	}
}

func createStringsResp2TestClient(t *testing.T, numCli int) ([]*StringsCache, func()) {
	var (
		caches     []*StringsCache
		serverAddr = os.Getenv("TEST_REDIS_ADDR")
		server     *miniredis.Miniredis
	)

	require.NotEmpty(t, serverAddr)

	// set pool
	for i := 0; i < numCli; i++ {

		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", serverAddr)
			},
			MaxActive: 100,
			MaxIdle:   100,
		}
		cli, err := NewStringsCache(StringsCacheConfig{
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
