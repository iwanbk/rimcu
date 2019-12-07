package rimcu

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	lru "github.com/hashicorp/golang-lru"
	"github.com/rs/xid"
	"github.com/shamaton/msgpack"
)

// listCache represents in memory cache of redis lists data type
//
// the sync mechanism is using a kind of oplog
// update list:
//	- update will always go to server.
//	- send listNotif (cmd, key, arg)
//	- after sending update command, the in mem cache will be marked as dirty
//
// read list:
//	- read ops always go to in mem cache except it marked as dirty
//
// dirty in mem cache
//	- in mem cache marked as dirty after it modified the respective key
//		it will be marked as clean when it receives notification of the respective operation
//	- when in dirty state, all ops of this key will be buffered
//	- the buffer will be executed when it receives notification of the respective operation
type listCache struct {
	pool     *redis.Pool
	clientID []byte

	cc *lru.Cache

	// lua scripts
	luaRpush *redis.Script //rpush
	luaLpop  *redis.Script //rpush

	logger Logger
}

func newListCache(pool *redis.Pool, size int, clientID []byte) (*listCache, error) {
	cc, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	if clientID == nil {
		clientID = xid.New().Bytes()
	}
	lc := &listCache{
		cc:       cc,
		pool:     pool,
		clientID: clientID,
		logger:   &debugLogger{},
		luaRpush: redis.NewScript(1, scriptRpush),
		luaLpop:  redis.NewScript(1, scriptLpop),
	}
	rns := newResp2NotifSubcriber(pool, lc.handleNotifData, lc.handleNotifDisconnect, listChannel)
	return lc, rns.runSubscriber()
}

// Rpush insert all the specified values at the tail of the list stored at key
// TODO: also do LTRIM
func (lc *listCache) Rpush(ctx context.Context, key, val string) error {
	// do RPUSH
	conn, err := lc.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send notif
	opID := xid.New().Bytes()
	notifMsg, err := newListNotif(lc.clientID, opID, listRpushCmd, key, val).Encode()
	if err != nil {
		return err
	}

	_, err = redis.String(lc.luaRpush.Do(conn, key, val, notifMsg))
	if err != nil {
		return err
	}

	// set dirty if cache exists
	cv, ok := lc.memGet(key)
	if ok {
		cv.setDirty(opID)
	}
	return nil
}

// Get the whole list.
//
// It gets directly from memory if the key state is not dirty
// otherwise will get from the server
func (lc *listCache) Get(ctx context.Context, key string) ([]string, error) {
	cv, ok := lc.memGet(key)
	if ok && !cv.isDirty() {
		return cv.list, nil
	}

	data, err := lc.getFromServer(ctx, key)
	if err != nil {
		return nil, err
	}

	if cv == nil {
		cv = &listCacheVal{}
		lc.memSet(key, cv)
	}

	// set cache
	cv.setList(data)
	return data, nil
}

// Lpop removes and returns the first element of the list stored at key.
//
// Lpop currently executes the command directly to the server.
func (lc *listCache) Lpop(ctx context.Context, key string) (string, bool, error) {
	// do RPUSH
	conn, err := lc.pool.GetContext(ctx)
	if err != nil {
		return "", false, err
	}
	defer conn.Close()

	// Send notif
	// TODO: make it one step with Lua script
	opID := xid.New().Bytes()
	notifMsg, err := newListNotif(lc.clientID, opID, listLpopCmd, key, "").Encode()
	if err != nil {
		return "", false, err
	}

	val, err := redis.String(lc.luaLpop.Do(conn, key, notifMsg))
	if err != nil {
		return "", false, fmt.Errorf("lpop failed: %v", err)
	}

	// update cache
	cv, ok := lc.memGet(key)
	if ok {
		cv.setDirty(opID)
	}

	return val, true, nil
}

func (lc *listCache) getFromServer(ctx context.Context, key string) ([]string, error) {
	// get ALL list data from the server
	conn, err := lc.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return redis.Strings(conn.Do("LRANGE", key, 0, -1))
}

func (lc *listCache) memGet(key string) (*listCacheVal, bool) {
	entry, ok := lc.cc.Get(key)
	if !ok {
		return nil, false
	}
	return entry.(*listCacheVal), true
}

func (lc *listCache) memSet(key string, cv *listCacheVal) {
	lc.cc.Add(key, cv)
}

// listNotif represents notification of the list write operation
type listNotif struct {
	ClientID []byte
	Cmd      string
	Key      string
	Arg      string
	OpID     []byte
}

func newListNotif(clientID, opID []byte, cmd, key, arg string) *listNotif {
	return &listNotif{
		ClientID: clientID,
		Cmd:      cmd,
		Key:      key,
		Arg:      arg,
		OpID:     opID,
	}
}

func (ln *listNotif) Encode() ([]byte, error) {
	return msgpack.Encode(ln)
}

func decodeListNotif(b []byte) (*listNotif, error) {
	var ln listNotif
	return &ln, msgpack.Decode(b, &ln)
}

func (lc *listCache) executeNotifs(cv *listCacheVal, notifs ...*listNotif) {
	for _, nt := range notifs {
		switch nt.Cmd {
		case listRpushCmd:
			lc.logger.Debugf("append list to key: %v", nt.Key)
			cv.pushBack(nt.Arg)
		case listLpopCmd:
			lc.logger.Errorf("pop list in key: %v", nt.Key)
			cv.front()
		default:
			lc.logger.Errorf("rimcu: unknown list notif command: %v", nt.Cmd)
		}
	}
}

// handleNotif handle raw notification from the redis
func (lc *listCache) handleNotifData(data []byte) {
	// decode notification
	nt, err := decodeListNotif(data)
	if err != nil {
		lc.logger.Errorf("failed to decode Notif:%w", err)
		return
	}

	//lc.logger.Debugf("got notif from %v", string(nt.ClientID))

	// if not in our memory, ignore
	cv, ok := lc.memGet(nt.Key)
	if !ok {
		//lc.logger.Debugf("[rimcu]%s ignore list to key: %v : key not stored in local cache", string(lc.clientID), nt.Key)
		return
	}

	// if dirty, buffer it
	if cv.isDirty() {
		if !bytes.Equal(cv.getDirtyID(), nt.OpID) {
			lc.logger.Debugf(("[rimcu]handleNotif: key cache is dirty, append notif"))
			cv.appendNotif(nt)
			return
		}

		// finally got the dirty opID, need to execute the buffered notif
		lc.executeNotifs(cv, cv.getNotifBuffer()...)
		lc.executeNotifs(cv, nt)
		cv.clearDirty()
		return
	}
	lc.executeNotifs(cv, nt)
}

func (lc *listCache) handleNotifDisconnect() {
	lc.logger.Errorf("TODO handleNotifDisconnect")
}

// listCacheVal represents in memory backing of the listCache
type listCacheVal struct {
	mtx         sync.RWMutex
	list        []string
	dirtyTs     int64
	dirtyID     []byte
	notifBuffer []*listNotif
}

func (lcv *listCacheVal) setList(data []string) {
	lcv.mtx.Lock()
	lcv.list = data
	lcv.mtx.Unlock()
}
func (lcv *listCacheVal) setDirty(opsID []byte) {
	lcv.mtx.Lock()
	lcv.dirtyTs = time.Now().Unix()
	lcv.dirtyID = opsID
	lcv.mtx.Unlock()
}

func (lcv *listCacheVal) isDirty() (isDirty bool) {
	lcv.mtx.RLock()

	isDirty = lcv.dirtyTs != 0

	lcv.mtx.RUnlock()
	return
}

func (lcv *listCacheVal) getDirtyID() (dirtyID []byte) {
	lcv.mtx.RLock()

	dirtyID = lcv.dirtyID

	lcv.mtx.RUnlock()
	return
}

func (lcv *listCacheVal) clearDirty() {
	lcv.mtx.Lock()
	lcv.dirtyTs = 0
	lcv.dirtyID = nil
	lcv.mtx.Unlock()
}

func (lcv *listCacheVal) appendNotif(nt *listNotif) {
	lcv.mtx.Lock()
	lcv.notifBuffer = append(lcv.notifBuffer, nt)
	lcv.mtx.Unlock()
}

func (lcv *listCacheVal) GetList() (list []string) {
	lcv.mtx.RLock()
	list = lcv.list
	lcv.mtx.RUnlock()
	return
}

func (lcv *listCacheVal) getNotifBuffer() []*listNotif {
	lcv.mtx.RLock()
	bufs := lcv.notifBuffer
	lcv.mtx.RUnlock()
	return bufs
}

func (lcv *listCacheVal) front() (string, bool) {
	lcv.mtx.Lock()
	defer lcv.mtx.Unlock()

	if len(lcv.list) == 0 {
		return "", false
	}
	var val string
	val, lcv.list = lcv.list[0], lcv.list[1:]
	return val, true
}

func (lcv *listCacheVal) pushBack(val string) {
	lcv.mtx.Lock()
	lcv.list = append(lcv.list, val)
	lcv.mtx.Unlock()
}

const (
	listChannel  = "rimcu:list"
	listRpushCmd = "RPUSH"
	listLpopCmd  = "LPOP"
)

const (
	scriptRpush = `
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('publish', 'rimcu:list', ARGV[2])
return 'OK'
	`
	scriptLpop = `
local val = redis.call('LPOP', KEYS[1])
redis.call('publish', 'rimcu:list', ARGV[1])
return val
	`
)
