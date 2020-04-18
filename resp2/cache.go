package resp2

import (
	"time"

	"github.com/bluele/gcache"
)

const (
	toRemoveConst = 1 // TODO: remove it
)

type cache struct {
	valCache gcache.Cache
}

type cacheVal struct {
	val      string
	clientID int64
}

func newCache(size int) *cache {
	c := &cache{}
	valCache := gcache.New(size).LRU().
		EvictedFunc(func(key, val interface{}) {
			c.evictedKeyHandler(key, val)
		}).Build()
	c.valCache = valCache

	return c
}

func (c *cache) evictedKeyHandler(key, val interface{}) {
	// remove record in the client -> key mapping
}

func (c *cache) Set(key, val string, clientID int64, expSecond int) {
	c.valCache.SetWithExpire(key, cacheVal{
		val:      val,
		clientID: clientID,
	}, time.Second*time.Duration(expSecond))
}

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

func (c *cache) Del(key string) {
	c.valCache.Remove(key)
}

func (c *cache) Clear() {
	c.valCache.Purge()
}
