package resp2

import (
	"context"
	"errors"

	"github.com/iwanbk/rimcu/internal/redigo/redis"
	"github.com/iwanbk/rimcu/logger"
)

var (
	// ErrNotFound returned when the given key is not exist
	ErrNotFound = errors.New("not found")
)

// StringsCache represents strings cache which use redis RESP2 protocol
// to synchronize data with the redis server.
type StringsCache struct {
	pool            *redis.Pool
	cc              *cache
	notifSubscriber *notifSubcriber
	logger          logger.Logger
}

// StringsCacheConfig is config for the StringsCache
type StringsCacheConfig struct {
	ServerAddr string

	// inmem cache max size
	CacheSize int

	// Logger for this lib, if nil will use Go log package which only print log on error
	Logger logger.Logger
}

// NewStringsCache creates new StringsCache object
func NewStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {

	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}

	sc := &StringsCache{
		logger: cfg.Logger,
		cc:     newCache(cfg.CacheSize),
	}

	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", cfg.ServerAddr, redis.DialCloseCb(sc.redisConnCloseCb))
		},
		MaxActive: 100,
		MaxIdle:   100,
	}
	sc.pool = pool

	sc.pool.DialCb = sc.dialCb

	sc.notifSubscriber = newNotifSubcriber(pool, sc.handleNotif, sc.handleNotifDisconnect, cfg.Logger)

	return sc, sc.notifSubscriber.runSubscriber()
}

// Close closes the cache, release all resources
func (sc *StringsCache) Close() error {
	sc.pool.Close()
	return nil
}

// Setex sets the value of the key with the given value and expiration in second.
//
// Calling this func will
// - invalidate inmem cache of other nodes
// - initialize in mem cache of this node
func (sc *StringsCache) Setex(ctx context.Context, key string, val interface{}, expSecond int) error {
	// get conn
	conn, err := sc.getConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = redis.String(conn.Do("SET", key, val, "EX", expSecond))
	if err != nil {
		return err
	}

	sc.cc.Set(key, val, conn.ClientID(), expSecond)
	return nil
}

// Get gets the value of the key.
//
// If the value not exists in the memory cache, it will try to get from the redis server
// and set the expiration to the given expSecond
func (sc *StringsCache) Get(ctx context.Context, key string, expSecond int) *StringResult {
	// try to get from in memory cache
	val, ok := sc.getMemCache(key)
	if ok {
		sc.logger.Debugf("GET: already in memcache")
		return newStringResult(val, nil)
	}

	// get from redis
	conn, err := sc.getConn(ctx)
	if err != nil {
		return newStringResult(nil, err)
	}
	defer conn.Close()

	//sc.logger.Debugf("CLIENT ID=%v", conn.ClientID())

	val, err = redis.String(conn.Do("GET", key))
	if err != nil {
		sc.logger.Debugf("GET failed: %v", err)
		if err == redis.ErrNil {
			err = ErrNotFound
		}
		return newStringResult(val, err)
	}

	// set to in-mem cache
	sc.cc.Set(key, val, conn.ClientID(), expSecond)

	return newStringResult(val, nil)
}

// Del deletes the key in both memory cache and redis server
func (sc *StringsCache) Del(ctx context.Context, key string) error {
	sc.cc.Del(key)

	// get from redis
	conn, err := sc.getConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("DEL", key)
	return err
}

func (sc *StringsCache) getMemCache(key string) (interface{}, bool) {
	return sc.cc.Get(key)
}

func (sc *StringsCache) getConn(ctx context.Context) (*redis.ActiveConn, error) {
	return sc.pool.GetContextWithCallback(ctx)
	// TODO: what if the pool dial callback failed? should we close this conn
}

func (sc *StringsCache) dialCb(ctx context.Context, conn redis.Conn) error {
	_, err := conn.Do("CLIENT", "TRACKING", "on", "REDIRECT", sc.notifSubscriber.clientID)

	if err != nil {
		sc.logger.Errorf("dial CB failed: %v", err)
	}

	return err
}

// redisConnCloseCb is callback to be called when the underlying redis connection
// is being closed.
//
// it deletes all keys belong to the given client
func (sc *StringsCache) redisConnCloseCb(clientID int64) {
	sc.cc.CleanCacheForConn(clientID)
}

// handle notif subscriber disconnected event
func (sc *StringsCache) handleNotifDisconnect() {
	sc.cc.Clear() // TODO : find other ways than complete clear like this
}

// handleNotif handle raw notification from the redis
func (sc *StringsCache) handleNotif(key string) {
	sc.cc.Del(key)
}
