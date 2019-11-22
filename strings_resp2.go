package rimcu

import (
	"bytes"
	"context"
	"fmt"

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

	// context
	cacheFinishedCh chan struct{}

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
		name:            xid.New().Bytes(),
		pool:            pool,
		logger:          logger,
		cc:              cc,
		cacheFinishedCh: make(chan struct{}),
		luaSetex:        redis.NewScript(1, scriptSetex),
		luaDel:          redis.NewScript(1, scriptDel),
	}
	return sc, sc.runSubscriber()
}

// Close closes the cache, release all resources
func (sc *StringsCacheResp2) Close() error {
	sc.cacheFinishedCh <- struct{}{}
	sc.pool.Close()
	return nil
}

// Setex sets the value of the key with the given value and expiration in second.
//
// / Calling this func will invalidate inmem cache of this key's slot in other nodes.
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

func (sc *StringsCacheResp2) runSubscriber() error {
	subscriberDoneCh, err := sc.startSub()
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-sc.cacheFinishedCh: // we are done
				return
			case <-subscriberDoneCh:
				// we're just disconnected from our Notif channel,
				// clear our in mem cache as we can't assume that the values
				// still updated
				sc.cc.Clear()

				// start new subscriber
				subscriberDoneCh, err = sc.startSub()
				if err != nil {
					sc.logger.Errorf("failed to start subscriber: %v", err)
				}
			}
		}
	}()
	return nil
}

// starts subscriber to listen to all of synchronization message sent by other nodes
func (sc *StringsCacheResp2) startSub() (chan struct{}, error) {
	doneCh := make(chan struct{})

	// setup subscriber
	sub, err := sc.subscribe(stringsnotifChannel)
	if err != nil {
		close(doneCh)
		return doneCh, err
	}

	// we're just connected to our Notif channel,
	// it means we previously not connected or disconnected from the Notif channel.
	// clear our in mem cache as we can't assume that the values
	// still updated
	// TODO: add some mechanism to prevent total clear like this.
	sc.cc.Clear()

	// run subscriber loop
	go func() {
		defer func() {
			close(doneCh)
			sub.Close()
		}()

		for {
			switch v := sub.Receive().(type) {
			case redis.Message:
				err := sc.handleNotif(v.Data)
				if err != nil {
					sc.logger.Errorf("handleNotif failed: %v", err)
					return
				}
			case error:
				// TODO: don't log if it is not error, i.e.: on cache Close
				sc.logger.Errorf("subscribe err: %v", v)
				return
			}
		}
	}()
	return doneCh, nil
}

// handleNotif handle raw notification from the redis
func (sc *StringsCacheResp2) handleNotif(data []byte) error {
	// decode notification
	nt, err := notif.Decode(data)
	if err != nil {
		return fmt.Errorf("failed to decode Notif:%w", err)
	}

	// ignore all updated from ourself
	if bytes.Equal(nt.ClientID, sc.name) {
		return nil
	}

	sc.logger.Debugf("[%s]msg from %s, slot:%v\n", sc.name, nt.ClientID, nt.Slot)

	sc.cc.HandleNotif(nt)
	return nil
}

// subscribe to the notification channel
func (sc *StringsCacheResp2) subscribe(channel string) (*redis.PubSubConn, error) {
	conn := sc.pool.Get()
	if err := conn.Err(); err != nil {
		return nil, err
	}
	sub := &redis.PubSubConn{Conn: conn}

	err := sub.Subscribe(stringsnotifChannel)
	if err != nil {
		sub.Close()
		return nil, err
	}
	return sub, nil
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
