package rimcu

import (
	"context"
	"strconv"
	"time"

	"github.com/iwanbk/rimcu/internal/resp3pool"
	"github.com/iwanbk/rimcu/logger"
	"github.com/karlseguin/ccache"
	"github.com/smallnest/resp3"
)

// StringsCache represents in memory strings cache which sync the cache
// with other nodes using Redis RESP3 protocol.
type StringsCache struct {
	pool *resp3pool.Pool

	// in memory cache
	cc *ccache.Cache

	// slots maps the redis slot into the cache key.
	slots *slot

	logger logger.Logger
}

// StringsCacheConfig represents config of StringsCache
type StringsCacheConfig struct {
	// redis server address
	ServerAddr string

	// size of the  in memory cache
	// Default is 100K
	CacheSize int

	// logger to be used, use default logger which print to stderr on error
	Logger logger.Logger
}

// NewStringsCache create strings cache with redis RESP3 protocol
func NewStringsCache(cfg StringsCacheConfig) *StringsCache {
	if cfg.CacheSize == 0 {
		cfg.CacheSize = 100000
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}

	sc := &StringsCache{
		cc:     ccache.New(ccache.Configure().MaxSize(1000)),
		slots:  newSlot(),
		logger: cfg.Logger,
	}
	poolCfg := resp3pool.PoolConfig{
		ServerAddr:   cfg.ServerAddr,
		InvalidateCb: sc.invalidate,
	}
	sc.pool = resp3pool.NewPool(poolCfg)
	return sc
}

// Setex sets the key to hold the string value with the given expiration second.
//
// Calling this func will invalidate inmem cache of this key's slot in other nodes.
func (sc *StringsCache) Setex(ctx context.Context, key, val string, exp int) error {
	_, err := sc.do(ctx, cmdSet, key, val, "EX", strconv.Itoa(exp))
	return err
}

// Get gets the value of key.
//
// It gets from the redis server only if the value not exists in memory cache,
// it then put the value from server in the in memcache with the given expiration
func (sc *StringsCache) Get(ctx context.Context, key string, exp int) (string, error) {
	// get from mem, if exists
	val, ok := sc.memGet(key)
	if ok {
		return val, nil
	}

	val, err := sc.getString(ctx, cmdGet, key)
	if err != nil {
		return "", err
	}

	// add to in mem cache
	sc.memSet(key, val, time.Duration(exp)*time.Second)

	return val, nil
}

// Del deletes the key in local and remote
func (sc *StringsCache) Del(ctx context.Context, key string) error {
	_, err := sc.do(ctx, cmdDel, key)
	if err != nil {
		return err
	}

	// delete from in mem cache
	sc.memDel(key)
	return nil
}

// TODO
func (sc *StringsCache) Close() error {
	return nil
}

// TODO: don't expose resp3.Value to this package
func (sc *StringsCache) do(ctx context.Context, cmd, key string, args ...string) (*resp3.Value, error) {
	conn, err := sc.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Do(cmd, key, args...)

	conn.Close()

	return resp, err
}

func (sc *StringsCache) getString(ctx context.Context, cmd, key string, args ...string) (string, error) {
	resp, err := sc.do(ctx, cmd, key, args...)
	if err != nil {
		return "", err
	}

	if resp.Str == "" {
		// TODO : differentiate between empty string and nil value
		// ErrNotFound must only be returned on nil value
		return "", ErrNotFound
	}
	return resp.Str, nil
}

// memSet sets the value of the given key.
//
// it also add the key to the slots map
func (sc *StringsCache) memSet(key, val string, exp time.Duration) {
	// add in cache
	sc.cc.Set(key, val, exp)

	// add this key in the slots
	sc.slots.addKey(key)
}

func (sc *StringsCache) memDel(key string) {
	sc.cc.Delete(key)
	sc.slots.removeKey(key)
}

func (sc *StringsCache) memGet(key string) (string, bool) {
	item := sc.cc.Get(key)
	if item == nil {
		return "", false
	}
	if item.Expired() {
		sc.cc.Delete(key)
		return "", false
	}

	return item.Value().(string), true
}

// invalidate the given slot
func (sc *StringsCache) invalidate(slot uint64) {
	se := sc.slots.removeSlot(slot)
	// delete all the keys from the memory cache
	for key := range se {
		sc.cc.Delete(key)
	}
}
