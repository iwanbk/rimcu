package resp2

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/iwanbk/rimcu/result"

	"github.com/iwanbk/rimcu/internal/cluster"
	"github.com/iwanbk/rimcu/internal/redigo/redis"
	"github.com/iwanbk/rimcu/logger"

	oriredigo "github.com/gomodule/redigo/redis"
)

var (
	// ErrNotFound returned when the given key is not exist
	ErrNotFound = errors.New("not found")
)

// StringsCache represents strings cache which use redis RESP2 protocol
// to synchronize data with the redis server.
type StringsCache struct {
	pool            *redis.Pool
	dataPool        DataPool
	cc              *cache
	notifSubscriber *notifSubcriber
	logger          logger.Logger
	mode            Mode
}

// DataPool is the pool that connect with the redis cluster proxy
type DataPool interface {
	GetContext(ctx context.Context) (oriredigo.Conn, error)
}

// StringsCacheConfig is config for the StringsCache
type StringsCacheConfig struct {
	ServerAddr string

	// inmem cache max size
	CacheSize int

	// inmem cache TTL
	CacheTTL int
	// Logger for this lib, if nil will use Go log package which only print log on error
	Logger logger.Logger

	// ClusterNodes is a list of cluster nodes
	// only being used by ProtoResp2ClusterProxy protocol.
	ClusterNodes []string

	Password string

	// Single or cluster-proxy
	Mode Mode

	// DataPool is the pool that connect with the redis cluster proxy
	DataPool DataPool
}

// Mode represents the mode of the cache
type Mode string

const (
	// ModeSingle is single redis mode
	ModeSingle Mode = "single"

	// ModeClusterProxy is a mode for redis cluster with front proxy like predixy
	ModeClusterProxy Mode = "cluster-proxy"
)

// NewStringsCache creates new StringsCache object
// TODO: support for rimcu's global pool
// some notes:
// - [single mode] when notif subscribed disconnected, all local cache cleared
// - [cluster-proxu] when notif subscriber of one node disconnected, all local cache cleared
func NewStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeSingle
	}

	// we don't allow empty pool in proxy mode
	if cfg.Mode == ModeClusterProxy {
		if cfg.DataPool == nil {
			return nil, fmt.Errorf("nil data pool not allowed in proxy mode")
		}
	}

	cfg.Logger.Debugf("cfg:%#v", cfg)

	sc := &StringsCache{
		logger: cfg.Logger,
		cc:     newCache(cfg.CacheSize),
		mode:   cfg.Mode,
	}

	if cfg.Mode == ModeSingle {
		// TODO: make the pool config taken from the config
		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", cfg.ServerAddr, redis.DialCloseCb(sc.redisConnCloseCb))
			},
			MaxActive: 100,
			MaxIdle:   100,
			DialCb:    sc.dialCb,
		}
		sc.pool = pool
	} else {
		sc.dataPool = cfg.DataPool
	}

	var (
		notifPools []*redis.Pool
		opts       = []redis.DialOption{redis.DialCloseCb(sc.redisConnCloseCb)}
	)
	if cfg.Password != "" {
		opts = append(opts, redis.DialPassword(cfg.Password))
	}

	notifHosts, err := getNotifHost(cfg)
	if err != nil {
		return nil, err
	}

	// [single mode] notif host is that single host
	// [cluster proxy] notif host is one node for each shard
	for _, clusterNode := range notifHosts {
		node := clusterNode
		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				cfg.Logger.Debugf("[notif]dialing: %v", node)
				return redis.Dial("tcp", node, opts...)
			},
		}
		notifPools = append(notifPools, pool)
	}

	sc.notifSubscriber = newNotifSubcriber(sc.handleNotif, sc.handleNotifDisconnect, sc.mode, cfg.Logger)

	return sc, sc.notifSubscriber.run(notifPools)
}

func getNotifHost(cfg StringsCacheConfig) ([]string, error) {
	if cfg.Mode == ModeSingle {
		return []string{cfg.ServerAddr}, nil
	}
	return getClusterMasters(cfg.ClusterNodes, cfg.Password)
}

func getClusterMasters(seeds []string, password string) ([]string, error) {
	ex := cluster.NewExplorer(seeds, password)
	ci, err := ex.Discover()
	if err != nil {
		return nil, err
	}
	return ci.Masters(), nil
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

	//sc.cc.Set(key, val, conn.ClientID(), expSecond)
	sc.cc.Del(key)
	return nil
}

// Get gets the value of the key.
//
// If the value not exists in the memory cache, it will try to get from the redis server
// and set the expiration to the given expSecond
func (sc *StringsCache) Get(ctx context.Context, key string, expSecond int) (result.StringsResult, error) {
	// try to get from in memory cache
	val, ok := sc.getMemCache(key)
	if ok {
		sc.logger.Debugf("GET: already in memcache")
		return newStringResult(val, true), nil
	}

	// get from redis
	conn, err := sc.getConn(ctx)
	if err != nil {
		log.Printf("failed to get conn:%v", err)
		return newStringResult(nil, false), err
	}
	defer conn.Close()

	val, err = conn.Do("GET", key)
	if err != nil || val == nil {
		sc.logger.Debugf("GET val:%v, err: %v", val, err)
		if err == redis.ErrNil {
			err = ErrNotFound
		}
		return newStringResult(val, false), err
	}

	// set to in-mem cache
	sc.cc.Set(key, val, getClientID(conn), expSecond)

	return newStringResult(val, false), nil
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

func (sc *StringsCache) getConn(ctx context.Context) (oriredigo.Conn, error) {
	if sc.mode == ModeSingle {
		return sc.pool.GetContextWithCallback(ctx)
	}
	return sc.dataPool.GetContext(ctx)
	// TODO: what if the pool dial callback failed? should we close this conn
}

func (sc *StringsCache) dialCb(ctx context.Context, conn redis.Conn) error {
	if sc.mode == ModeClusterProxy {
		return nil
	}
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
	sc.logger.Debugf("[rimcu]got notif: %v", key)
	sc.cc.Del(key)
}

func getClientID(connObj interface{}) int64 {
	if conn, ok := connObj.(*redis.ActiveConn); ok {
		return conn.ClientID()
	}
	return 0
}
