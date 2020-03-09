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
		logger: cfg.Logger,
	}
	poolCfg := resp3pool.PoolConfig{
		ServerAddr:   cfg.ServerAddr,
		InvalidateCb: sc.invalidate,
		Logger:       sc.logger,
	}
	sc.pool = resp3pool.NewPool(poolCfg)
	return sc
}

// StringValue defines string with Nil flag.
type StringValue struct {
	Nil bool
	Val string
}

// Setex sets the key to hold the string value with the given expiration second.
//
// Calling this func will invalidate inmem cache of this key's slot in other nodes.
func (sc *StringsCache) Setex(ctx context.Context, key, val string, exp int) error {
	return sc.write(ctx, cmdSet, key, val, "EX", strconv.Itoa(exp))
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
	return sc.write(ctx, cmdDel, key)
}

// Append value to the end of the key
func (sc *StringsCache) Append(ctx context.Context, key, val string) error {
	return sc.write(ctx, cmdAppend, key, val)
}

// MSet set multiple key values at once.
//
// The format of the values:
//
// - key1, val1, key2, val2, ....
func (sc *StringsCache) MSet(ctx context.Context, values ...string) error {
	lenVal := len(values)
	//check argument
	if lenVal == 0 || (lenVal%2 != 0) {
		return ErrInvalidArgs
	}

	_, err := sc._do(ctx, cmdMSet, values...)
	if err != nil {
		return err
	}

	// del inmemcache
	for i, val := range values {
		if i%2 == 0 {
			sc.memDel(val)
		}
	}

	return nil

}

// MGet get values of multiple keys at once.
//
// if the key exists, it will cached in the memory cache with exp seconds expiration time
func (sc *StringsCache) MGet(ctx context.Context, exp int, keys ...string) ([]StringValue, error) {
	var (
		getKeys    []string // keys to get from the server
		getIndexes []int    // index of the key to get from the server
		results    = make([]StringValue, len(keys))
		tsExp      = time.Duration(exp) * time.Second
	)

	// pick only keys that not exist in the cache
	for i, key := range keys {
		// check in mem
		val, ok := sc.memGet(key)
		if ok {
			results[i] = StringValue{
				Nil: false,
				Val: val,
			}
			continue
		}
		getKeys = append(getKeys, key)
		getIndexes = append(getIndexes, i)
	}

	resp, err := sc._do(ctx, cmdMGet, getKeys...)
	if err != nil {
		return nil, err
	}

	for i, elem := range resp.Elems {
		strVal := StringValue{
			Nil: sc.isNullString(elem),
			Val: elem.Str,
		}
		results[getIndexes[i]] = strVal
		if !strVal.Nil {
			sc.memSet(getKeys[i], strVal.Val, tsExp)
		}
	}
	return results, nil
}

func (sc *StringsCache) write(ctx context.Context, cmd, key string, args ...string) error {
	_, err := sc.do(ctx, cmd, key, args...)
	if err != nil {
		return err
	}

	// delete from in mem cache
	sc.memDel(key)
	return nil
}

// Close the strings cache and release it's all resources
func (sc *StringsCache) Close() error {
	sc.pool.Close()
	return nil
}

// TODO: don't expose resp3.Value to this package
func (sc *StringsCache) do(ctx context.Context, cmd, key string, args ...string) (*resp3.Value, error) {
	return sc._do(ctx, cmd, append([]string{key}, args...)...)
}

func (sc *StringsCache) _do(ctx context.Context, cmd string, args ...string) (*resp3.Value, error) {

	conn, err := sc.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Do(ctx, cmd, args...)

	conn.Close()

	return resp, err
}

func (sc *StringsCache) getString(ctx context.Context, cmd, key string, args ...string) (string, error) {
	resp, err := sc.do(ctx, cmd, key, args...)
	if err != nil {
		return "", err
	}

	if sc.isNullString(resp) {
		return "", ErrNotFound
	}

	return resp.Str, nil
}

func (sc *StringsCache) isNullString(resp *resp3.Value) bool {
	return resp.Type == '_'
}

// memSet sets the value of the given key.
//
// it also add the key to the slots map
func (sc *StringsCache) memSet(key, val string, exp time.Duration) {
	// add in cache
	sc.cc.Set(key, val, exp)
}

func (sc *StringsCache) memDel(key string) {
	sc.cc.Delete(key)
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
func (sc *StringsCache) invalidate(key string) {
	sc.memDel(key)
}
