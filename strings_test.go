package rimcu

import (
	"context"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

var (
	testRedis6ServerAddr = "localhost:6379"
	testExp              = 1000
	syncTimeWait         = 1 * time.Second
)

// Set will not init inmem cache
func TestStringsCache_Set_NotInitInMem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	scs, cleanup := createStringsCacheTestClient(t, 1)
	defer cleanup()

	sc1 := scs[0]

	var (
		key1 = xid.New().String()
		val1 = "val_1"
	)

	err := sc1.Setex(ctx, key1, val1, testExp)
	require.NoError(t, err)

	_, ok := sc1.memGet(key1)
	require.False(t, ok)
}

// Test that Set will invalidate memcache in other nodes
func TestStringsCache_Set_Invalidate(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsCacheTestClient(t, 3)
	defer cleanup()

	var (
		sc1, sc2, sc3 = scs[0], scs[1], scs[2]
		key1          = generateRandomKey()
		val1          = "val_1"
		val2          = "val_2"
	)

	{ // Test initialization, get the value to activate listening
		// Set
		err := sc1.Setex(ctx, key1, val1, testExp)
		require.NoError(t, err)

		// Get to activate listening
		_, err = sc2.Get(ctx, key1, testExp)
		require.NoError(t, err)

		_, err = sc3.Get(ctx, key1, testExp)
		require.NoError(t, err)
	}

	// make sure initial condition, key1 must exists in memcache
	{
		_, ok := sc2.memGet(key1)
		require.True(t, ok)

		_, ok = sc3.memGet(key1)
		require.True(t, ok)
	}

	// do the action : Set
	{
		// set
		err := sc1.Setex(ctx, key1, val2, testExp)
		require.NoError(t, err)
	}
	time.Sleep(syncTimeWait)

	// check expected condition
	{

		// check
		_, ok := sc2.memGet(key1)
		require.False(t, ok)

		_, ok = sc3.memGet(key1)
		require.False(t, ok)
	}

}

// Get valid key must initiate inmem cache
func TestStringsCache_Get_Valid_InitInMem(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsCacheTestClient(t, 3)
	defer cleanup()

	var (
		sc1, sc2, sc3 = scs[0], scs[1], scs[2]
		key1          = generateRandomKey()
		val1          = "val_1"
	)

	// Test initialization set the key in redis
	{
		// set
		err := sc1.Setex(ctx, key1, val1, testExp)
		require.NoError(t, err)

		val, err := sc1.Get(ctx, key1, testExp)
		require.NoError(t, err)
		require.Equal(t, val1, val)
	}

	// make sure initial condition,
	{
		_, ok := sc2.memGet(key1)
		require.False(t, ok)

		_, ok = sc3.memGet(key1)
		require.False(t, ok)
	}

	// do the action : Get
	{
		// get
		val, err := sc2.Get(ctx, key1, testExp)
		require.NoError(t, err)
		require.Equal(t, val1, val)

		val, err = sc3.Get(ctx, key1, testExp)
		require.NoError(t, err)
		require.Equal(t, val1, val)
	}

	time.Sleep(syncTimeWait)

	// check expected condition
	// key1 exists in memcache
	{

		// check
		_, ok := sc1.memGet(key1)
		require.True(t, ok)

		_, ok = sc2.memGet(key1)
		require.True(t, ok)

		_, ok = sc3.memGet(key1)
		require.True(t, ok)
	}

}

// Get invalid key must:
// -  not initiate in mem cache
// - got error
func TestStringsCache_Get_Invalid_NotInitInMem(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsCacheTestClient(t, 3)
	defer cleanup()

	var (
		sc1  = scs[0]
		key1 = generateRandomKey()
	)

	// make sure initial condition, key1 must not exists in memcache
	{
		_, ok := sc1.memGet(key1)
		require.False(t, ok)
	}

	// do the action :
	{
		// get
		_, err := sc1.Get(ctx, key1, testExp)
		require.Error(t, err)
	}

	// check expected condition
	// key1 not exists in memcache
	{

		// check
		_, ok := sc1.memGet(key1)
		require.False(t, ok)

	}

}

// Test Del : deleting valid key must propagate to other nodes
func TestStringsCache_Del_ValidKey_Propagate(t *testing.T) {
	ctx := context.Background()

	scs, cleanup := createStringsCacheTestClient(t, 3)
	defer cleanup()

	var (
		sc1, sc2, sc3 = scs[0], scs[1], scs[2]
		key1          = generateRandomKey()
		val1          = "val_1"
	)

	{ // Test initialization, get the value to activate listening
		// Set
		err := sc1.Setex(ctx, key1, val1, testExp)
		require.NoError(t, err)

		// Get to activate listening
		_, err = sc2.Get(ctx, key1, testExp)
		require.NoError(t, err)

		_, err = sc3.Get(ctx, key1, testExp)
		require.NoError(t, err)
	}

	// make sure initial condition, key1 must exists in memcache
	{
		_, ok := sc2.memGet(key1)
		require.True(t, ok)

		_, ok = sc3.memGet(key1)
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

		// check
		_, ok := sc1.memGet(key1)
		require.False(t, ok)

		_, ok = sc2.memGet(key1)
		require.False(t, ok)

		_, ok = sc3.memGet(key1)
		require.False(t, ok)
	}
}

func generateRandomKey() string {
	return xid.New().String()
}

func createStringsCacheTestClient(t *testing.T, numCli int) ([]*StringsCache, func()) {
	var caches []*StringsCache

	for i := 0; i < numCli; i++ {
		sc := NewStringsCache(StringsCacheConfig{
			ServerAddr: testRedis6ServerAddr,
		})
		caches = append(caches, sc)
	}

	return caches, func() {
		for _, cli := range caches {
			cli.Close()
		}
	}
}
