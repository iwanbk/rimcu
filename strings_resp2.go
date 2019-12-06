package rimcu

import (
	"bytes"
	"context"
	"github.com/gomodule/redigo/redis"
	"github.com/iwanbk/rimcu/internal/inmemcache/keycache"
	"github.com/iwanbk/rimcu/internal/notif"
	"github.com/rs/xid"
)

// StringsCacheResp2 represents strings cache which use redis RESP2 protocol
// to synchronize data with the redis server.
type StringsCacheResp2 struct {
	pool *redis.Pool

	// Auto generated unique client name, needed as synchronization identifier
	// TODO generate name of the client using `CLIENT ID` redis command
	name []byte

	cc *keycache.KeyCache

	// lua scripts
	luaSetex *redis.Script //setex command
	luaDel   *redis.Script // del command

	logger Logger
}

// StringsCacheResp2Config is config for the StringsCacheResp2
type StringsCacheResp2Config struct {
	// inmem cache max size
	CacheSize int

	// Logger for this lib, if nil will use Go log package which only print log on error
	Logger Logger
}

// NewStringsCacheResp2 creates new StringsCacheResp2 object
func NewStringsCacheResp2(cfg StringsCacheResp2Config, pool *redis.Pool) (*StringsCacheResp2, error) {
	cc, err := keycache.New(cfg.CacheSize)
	if err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = &defaultLogger{}
	}

	sc := &StringsCacheResp2{
		name:     xid.New().Bytes(),
		pool:     pool,
		logger:   logger,
		cc:       cc,
		luaSetex: redis.NewScript(1, scriptSetex),
		luaDel:   redis.NewScript(1, scriptDel),
	}
	rns := newResp2NotifSubcriber(pool, sc.handleNotif, sc.handleNotifDisconnect, stringsnotifChannel)
	return sc, rns.runSubscriber()
}

// Close closes the cache, release all resources
func (sc *StringsCacheResp2) Close() error {
	sc.pool.Close()
	return nil
}

// Setex sets the value of the key with the given value and expiration in second.
//
// Calling this func will
// - invalidate inmem cache of other nodes
// - initialize in mem cache of this node
func (sc *StringsCacheResp2) Setex(ctx context.Context, key, val string, expSecond int) error {
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
func (sc *StringsCacheResp2) Get(ctx context.Context, key string, expSecond int) (string, error) {
	// try to get from in memory cache
	val, ok := sc.getMemCache(key)
	if ok {
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
func (sc *StringsCacheResp2) Del(ctx context.Context, key string) error {
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

func (sc *StringsCacheResp2) setMemCache(key, val string, expSecond int) {
	sc.cc.SetEx(key, val, expSecond)
}

func (sc *StringsCacheResp2) getMemCache(key string) (string, bool) {
	return sc.cc.Get(key)
}

// handle notif subscriber disconnected event
func (sc *StringsCacheResp2) handleNotifDisconnect() {
	sc.cc.Clear() // TODO : find other ways than complete clear like this
}

// handleNotif handle raw notification from the redis
func (sc *StringsCacheResp2) handleNotif(data []byte) {
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

	sc.logger.Debugf("[%s]msg from %s, slot:%v\n", sc.name, nt.ClientID, nt.Slot)

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
