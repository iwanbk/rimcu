package resp2

import (
	"context"
	"errors"

	"github.com/iwanbk/rimcu/internal/redigo/redis"
	"github.com/iwanbk/rimcu/logger"
	"github.com/rs/xid"
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

	// Auto generated unique client name, needed as synchronization identifier
	// TODO generate name of the client using `CLIENT ID` redis command
	name []byte
}

// StringsCacheConfig is config for the StringsCache
type StringsCacheConfig struct {
	ServerAddr string

	// inmem cache max size
	CacheSize int

	// Logger for this lib, if nil will use Go log package which only print log on error
	Logger logger.Logger

	// ID of the cache client, leave it to nil will generate unique value
	ClientID []byte
}

// NewStringsCache creates new StringsCache object
func NewStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {

	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}

	if cfg.ClientID == nil {
		cfg.ClientID = xid.New().Bytes()
	}

	sc := &StringsCache{
		name:   cfg.ClientID,
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
func (sc *StringsCache) Setex(ctx context.Context, key, val string, expSecond int) error {
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
func (sc *StringsCache) Get(ctx context.Context, key string, expSecond int) (string, error) {
	// try to get from in memory cache
	val, ok := sc.getMemCache(key)
	if ok {
		sc.logger.Debugf("[%s] GET: already in memcache", string(sc.name))
		return val, nil
	}

	// get from redis
	conn, err := sc.getConn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	//sc.logger.Debugf("CLIENT ID=%v", conn.ClientID())

	val, err = redis.String(conn.Do("GET", key))
	if err != nil {
		sc.logger.Debugf("[%s] GET failed: %v", string(sc.name), err)
		if err == redis.ErrNil {
			err = ErrNotFound
		}
		return "", err
	}

	// set to in-mem cache
	sc.cc.Set(key, val, conn.ClientID(), expSecond)

	return val, nil
}

func (sc *StringsCache) getMemCache(key string) (string, bool) {
	return sc.cc.Get(key)
}

func (sc *StringsCache) getConn(ctx context.Context) (*redis.ActiveConn, error) {
	return sc.pool.GetContextWithCallback(ctx)
	// TODO: what if the pool dial callback failed? should we close this conn
}

func (sc *StringsCache) dialCb(ctx context.Context, conn redis.Conn) error {
	//sc.logger.Debugf("executing dial CB")
	trackClientID := sc.notifSubscriber.clientID
	//sc.logger.Debugf("redirect conn to client ID:%v", trackClientID)
	_, err := conn.Do("CLIENT", "TRACKING", "on", "REDIRECT", trackClientID)

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
