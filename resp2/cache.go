package resp2

import (
	"time"

	"github.com/bluele/gcache"
)

type cache struct {
	valCache gcache.Cache
	ckm      *connKeyMap
}

type cacheVal struct {
	val      string
	clientID int64 // TODO: move this info to `ckm`
}

func newCache(size int) *cache {
	c := &cache{
		ckm: newConnKeyMap(),
	}

	valCache := gcache.New(size).LRU().
		EvictedFunc(func(key, val interface{}) {
			c.evictedKeyHandler(key, val)
		}).Build()
	c.valCache = valCache

	return c
}

func (c *cache) evictedKeyHandler(key, val interface{}) {
	// remove record in the client -> key mapping
	cVal, ok := val.(cacheVal)
	if !ok {
		panic("]evictedKeyHandler] unpexpected type of cache value")
	}
	c.ckm.del(cVal.clientID, cVal.val)
}

// Set cache
func (c *cache) Set(key, val string, clientID int64, expSecond int) {
	c.ckm.add(clientID, key)
	c.valCache.SetWithExpire(key, cacheVal{
		val:      val,
		clientID: clientID,
	}, time.Second*time.Duration(expSecond))
}

// Get cache
func (c *cache) Get(key string) (string, bool) {
	val, err := c.valCache.Get(key)
	if err != nil {
		return "", false
	}

	cVal, ok := val.(cacheVal)
	if !ok {
		return "", false
	}

	return cVal.val, true
}

// Del cache
func (c *cache) Del(key string) {
	val, err := c.valCache.Get(key)
	if err != nil {
		return
	}

	cVal, ok := val.(cacheVal)
	if !ok {
		return
	}

	c.valCache.Remove(key)
	c.ckm.del(cVal.clientID, key)
}

func (c *cache) CleanCacheForConn(clientID int64) {
	// clean all keys in cache
	keys := c.ckm.keys(clientID)
	for key := range keys {
		c.Del(key)
	}
	// clean conn<->key mapping
	c.ckm.clean(clientID)
}

func (c *cache) Clear() {
	c.valCache.Purge()
}
