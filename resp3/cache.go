package resp3

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/iwanbk/rimcu/result"

	"github.com/iwanbk/resp3"
	"github.com/iwanbk/rimcu/internal/resp3pool"
	"github.com/iwanbk/rimcu/logger"
	"github.com/karlseguin/ccache"
)

var (
	// ErrNotFound returned when the value of the key is not exists
	ErrNotFound = errors.New("not found")

	// ErrInvalidArgs returned when the user pass invalid arguments to the func
	ErrInvalidArgs = errors.New("invalid arguments")
)

// Cache represents in memory cache which sync the cache
// with other nodes using Redis RESP3 protocol.
type Cache struct {
	pool *resp3pool.Pool

	// in memory cache
	memcache *ccache.Cache

	logger logger.Logger
}

// Config represents config of Cache
type Config struct {
	// redis server address
	ServerAddr string

	// size of the  in memory cache
	// Default is 100K
	CacheSize int

	// logger to be used, use default logger which print to stderr on error
	Logger logger.Logger
}

// New create strings cache with redis RESP3 protocol
func New(cfg Config) *Cache {
	if cfg.CacheSize == 0 {
		cfg.CacheSize = 100000
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}

	sc := &Cache{
		memcache: ccache.New(ccache.Configure().MaxSize(1000)),
		logger:   cfg.Logger,
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
func (c *Cache) Setex(ctx context.Context, key string, val interface{}, exp int) error {
	return c.write(ctx, cmdSet, key, val, "EX", strconv.Itoa(exp))
}

// Get gets the value of key.
//
// It gets from the redis server only if the value not exists in memory cache,
// it then put the value from server in the in memcache with the given expiration
func (c *Cache) Get(ctx context.Context, key string, exp int) (result.StringsResult, error) {
	// get from mem, if exists
	val, ok := c.memGet2(key)
	if ok {
		return newStringsResult(val, true), nil
	}

	resp, err := c.get(ctx, cmdGet, key)
	if err != nil {
		return nil, err
	}
	val = cacheVal{
		typ: cacheTypString, // TODO : fix it, not all values are in string type
		val: resp.Str,
	}

	// add to in mem cache
	c.memSet(key, val, time.Duration(exp)*time.Second)

	return newStringsResult(val, false), nil
}

// Del deletes the key in local and remote
func (c *Cache) Del(ctx context.Context, key string) error {
	return c.write(ctx, cmdDel, key)
}

// Append value to the end of the key
func (c *Cache) Append(ctx context.Context, key, val string) error {
	return c.write(ctx, cmdAppend, key, val)
}

// MSet set multiple key values at once.
//
// The format of the values:
//
// - key1, val1, key2, val2, ....
func (c *Cache) MSet(ctx context.Context, values ...interface{}) error {
	lenVal := len(values)
	//check argument
	if lenVal == 0 || (lenVal%2 != 0) {
		return ErrInvalidArgs
	}

	_, err := c._do(ctx, cmdMSet, values...)
	if err != nil {
		return err
	}

	// del inmemcache
	for i, val := range values {
		if i%2 == 0 {
			c.memDel(fmt.Sprintf("%s", val))
		}
	}

	return nil

}

// MGet get values of multiple keys at once.
//
// if the key exists, it will cached in the memory cache with exp seconds expiration time
func (c *Cache) MGet(ctx context.Context, exp int, keys ...string) ([]StringValue, error) {
	var (
		getKeys    []interface{} // keys to get from the server
		getIndexes []int         // index of the key to get from the server
		results    = make([]StringValue, len(keys))
		tsExp      = time.Duration(exp) * time.Second
	)

	// pick only keys that not exist in the cache
	for i, key := range keys {
		// check in mem
		val, ok := c.memGet(key)
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

	resp, err := c._do(ctx, cmdMGet, getKeys...)
	if err != nil {
		return nil, err
	}

	for i, elem := range resp.Elems {
		strVal := StringValue{
			Nil: c.isNullString(elem),
			Val: elem.Str,
		}
		results[getIndexes[i]] = strVal
		if !strVal.Nil {
			c.memSet(fmt.Sprintf("%s", (getKeys[i])), strVal.Val, tsExp)
		}
	}
	return results, nil
}

func (c *Cache) write(ctx context.Context, cmd, key string, args ...interface{}) error {
	_, err := c.do(ctx, cmd, key, args...)
	if err != nil {
		return err
	}

	// delete from in mem cache
	c.memDel(key)
	return nil
}

// Close the strings cache and release it's all resources
func (c *Cache) Close() error {
	c.pool.Close()
	return nil
}

// TODO: don't expose resp3.Value to this package
func (c *Cache) do(ctx context.Context, cmd, key interface{}, args ...interface{}) (*resp3.Value, error) {
	return c._do(ctx, cmd, append([]interface{}{key}, args...)...)
}

func (c *Cache) _do(ctx context.Context, cmd interface{}, args ...interface{}) (*resp3.Value, error) {

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	resp, err := conn.Do(ctx, cmd, args...)

	return resp, err
}

func (c *Cache) get(ctx context.Context, cmd, key interface{}, args ...interface{}) (*resp3.Value, error) {
	resp, err := c.do(ctx, cmd, key, args...)
	if err != nil {
		return nil, err
	}

	if c.isNullString(resp) {
		return nil, ErrNotFound
	}

	return resp, nil
}

func (c *Cache) isNullString(resp *resp3.Value) bool {
	return resp.Type == '_'
}

// memSet sets the value of the given key.
//
// it also add the key to the slots map
func (c *Cache) memSet(key string, val interface{}, exp time.Duration) {
	// add in cache
	c.memcache.Set(key, val, exp)
}

func (c *Cache) memDel(key string) {
	c.memcache.Delete(key)
}

func (c *Cache) memGet2(key string) (cacheVal, bool) {
	item := c.memcache.Get(key)
	if item == nil {
		return cacheVal{}, false
	}
	if item.Expired() {
		c.memcache.Delete(key)
		return cacheVal{}, false
	}

	return item.Value().(cacheVal), true
}

func (c *Cache) memGet(key string) (string, bool) {
	item := c.memcache.Get(key)
	if item == nil {
		return "", false
	}
	if item.Expired() {
		c.memcache.Delete(key)
		return "", false
	}

	return item.Value().(string), true
}

// invalidate the given slot
func (c *Cache) invalidate(key string) {
	c.memDel(key)
}

const (
	cmdSet    = "SET"
	cmdGet    = "GET"
	cmdDel    = "DEL"
	cmdAppend = "APPEND"
	cmdMSet   = "MSET"
	cmdMGet   = "MGET"
)
