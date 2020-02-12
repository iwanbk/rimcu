package resp2

import (
	"bytes"
	"context"
	"errors"

	"github.com/gomodule/redigo/redis"
	"github.com/iwanbk/rimcu/internal/inmemcache/keycache"
	"github.com/iwanbk/rimcu/internal/notif"
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
	pool *redis.Pool

	// Auto generated unique client name, needed as synchronization identifier
	// TODO generate name of the client using `CLIENT ID` redis command
	name []byte

	cc *keycache.KeyCache

	// lua scripts
	luaSetex *redis.Script //setex command
	luaDel   *redis.Script // del command

	logger logger.Logger
}

// StringsCacheResp2Config is config for the StringsCache
type StringsCacheResp2Config struct {
	// inmem cache max size
	CacheSize int

	// Logger for this lib, if nil will use Go log package which only print log on error
	Logger logger.Logger

	// ID of the cache client, leave it to nil will generate unique value
	ClientID []byte
}

// NewStringsCacheResp2 creates new StringsCache object
func NewStringsCacheResp2(cfg StringsCacheResp2Config, pool *redis.Pool) (*StringsCache, error) {
	cc, err := keycache.New(cfg.CacheSize)
	if err != nil {
		return nil, err
	}

	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}

	if cfg.ClientID == nil {
		cfg.ClientID = xid.New().Bytes()
	}

	sc := &StringsCache{
		name:     cfg.ClientID,
		pool:     pool,
		logger:   cfg.Logger,
		cc:       cc,
		luaSetex: redis.NewScript(1, scriptSetex),
		luaDel:   redis.NewScript(1, scriptDel),
	}
	rns := newResp2NotifSubcriber(pool, sc.handleNotif, sc.handleNotifDisconnect,
		stringsnotifChannel, cfg.Logger)
	return sc, rns.runSubscriber()
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
	conn, err := sc.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// set to redis
	notif, err := sc.cc.NewNotif(sc.name, key).Encode()
	if err != nil {
		return err
	}

	_, err = redis.String(sc.luaSetex.Do(conn, key, expSecond, val, notif))
	if err != nil {
		return err
	}

	sc.setMemCache(key, val, expSecond)
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
	conn, err := sc.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	val, err = redis.String(conn.Do("GET", key))
	if err != nil {
		sc.logger.Debugf("[%s] GET failed: %v", string(sc.name), err)
		if err == redis.ErrNil {
			err = ErrNotFound
		}
		return "", err
	}

	// set to in-mem cache
	sc.setMemCache(key, val, expSecond)

	return val, nil
}

// Del deletes the cache of the given key
func (sc *StringsCache) Del(ctx context.Context, key string) error {
	sc.cc.Del(key)

	// get conn
	conn, err := sc.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	notif, err := sc.cc.NewNotif(sc.name, key).Encode()
	if err != nil {
		return err
	}
	_, err = redis.String(sc.luaDel.Do(conn, key, notif))
	return err
}

func (sc *StringsCache) setMemCache(key, val string, expSecond int) {
	sc.logger.Debugf("[%s] setMemCache %s", string(sc.name), key)
	sc.cc.SetEx(key, val, expSecond)
}

func (sc *StringsCache) getMemCache(key string) (string, bool) {
	return sc.cc.Get(key)
}

// handle notif subscriber disconnected event
func (sc *StringsCache) handleNotifDisconnect() {
	sc.cc.Clear() // TODO : find other ways than complete clear like this
}

// handleNotif handle raw notification from the redis
func (sc *StringsCache) handleNotif(data []byte) {
	// decode notification
	nt, err := notif.Decode(data)
	if err != nil {
		sc.logger.Errorf("failed to decode Notif:%w", err)
		return
	}

	// ignore all updated from ourself
	if bytes.Equal(nt.ClientID, sc.name) {
		return
	}

	sc.logger.Debugf("[%s]msg from %s, key:%v\n", string(sc.name), string(nt.ClientID), nt.Key)

	sc.cc.HandleNotif(nt)
}

const (
	scriptSetex = `
local setret = redis.call('setex', KEYS[1], ARGV[1], ARGV[2])
redis.call('publish', 'rimcu:strings', ARGV[3])
return 'OK'
			`
	scriptDel = `
local setret = redis.call('del', KEYS[1])
redis.call('publish', 'rimcu:strings', ARGV[1])
return '1'
			`
)

const (
	stringsnotifChannel = "rimcu:strings"
)
