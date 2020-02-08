package resp2

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

	opIDGen opIDGenerator

	// lua scripts
	luaRpush *redis.Script //rpush
	luaLpop  *redis.Script //rpush

	logger Logger
}

type ListCacheResp2Config struct {
	Logger   Logger
	Size     int
	ClientID []byte
	opIDGen  opIDGenerator
}

func newListCacheResp2(pool *redis.Pool, cfg ListCacheResp2Config) (*listCache, error) {
	if cfg.Size <= 0 {
		cfg.Size = 1000
	}
	if cfg.ClientID == nil {
		cfg.ClientID = xid.New().Bytes()
	}
	if cfg.Logger == nil {
		cfg.Logger = &defaultLogger{}
	}
	if cfg.opIDGen == nil {
		cfg.opIDGen = &defaultOpIDgen{}
	}
	cc, err := lru.New(cfg.Size)
	if err != nil {
		return nil, err
	}
	lc := &listCache{
		cc:       cc,
		pool:     pool,
		clientID: cfg.ClientID,
		logger:   cfg.Logger,
		opIDGen:  cfg.opIDGen,
		luaRpush: redis.NewScript(1, scriptRpush),
		luaLpop:  redis.NewScript(1, scriptLpop),
	}
	rns := newResp2NotifSubcriber(pool, lc.handleNotifData, lc.handleNotifDisconnect, listChannel)
	return lc, rns.runSubscriber()
}

// Rpush insert all the specified values at the tail of the list stored at key
// TODO:
//  - also do LTRIM
//  - accept not only one values
func (lc *listCache) Rpush(ctx context.Context, key, val string) error {
	notif := newListNotif(lc.clientID, lc.opIDGen.Generate(), listRpushCmd, key, val)

	_, err := redis.String(lc.execLua(ctx, lc.luaRpush, notif, key, val))
	return err
}

// Get the whole list.
//
// It gets directly from memory if the key state is not dirty
// otherwise will get from the server
func (lc *listCache) Get(ctx context.Context, key string) ([]string, error) {
	cv, ok := lc.memGet(key)
	if ok && !cv.IsDirty() {
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
	cv.SetList(data)
	return data, nil
}

// Lpop removes and returns the first element of the list stored at key.
//
// Lpop currently executes the command directly to the server.
func (lc *listCache) Lpop(ctx context.Context, key string) (string, bool, error) {

	notif := newListNotif(lc.clientID, lc.opIDGen.Generate(), listLpopCmd, key, "")

	val, err := redis.String(lc.execLua(ctx, lc.luaLpop, notif, key))
	if err != nil {
		return "", false, fmt.Errorf("lpop failed: %v", err)
	}

	return val, true, nil
}

func (lc *listCache) execLua(ctx context.Context, scr *redis.Script, nt *listNotif, key string, args ...interface{}) (interface{}, error) {
	notifMsg, err := nt.Encode()
	if err != nil {
		return nil, err
	}
	conn, err := lc.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cv, hasCache := lc.memGet(key)
	if hasCache {
		cv.SetDirty(nt.OpID)
	}

	scrArgs := append([]interface{}{key, notifMsg}, args...)
	val, err := scr.Do(conn, scrArgs...)

	if err != nil && hasCache {
		cv.ClearDirty(nt.OpID)
	}

	return val, err
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

func (lc *listCache) name() string {
	return string(lc.clientID)
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
		lc.logger.Debugf("rimcu:[%s]op=%v `%s` to key: %v", lc.name(), string(nt.OpID), nt.Cmd, nt.Key)
		switch nt.Cmd {
		case listRpushCmd:
			cv.pushBack(nt.Arg)
		case listLpopCmd:
			cv.front()
		default:
			lc.logger.Errorf("rimcu:[%s]unknown list notif command: %v", lc.name(), nt.Cmd)
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

	// if not in our memory, ignore
	cv, ok := lc.memGet(nt.Key)
	if !ok {
		//lc.logger.Debugf("[rimcu]%s ignore list to key: %v : key not stored in local cache", string(lc.clientID), nt.Key)
		return
	}
	cv.mtx.Lock()
	defer cv.mtx.Unlock()

	// if dirty, buffer it
	if cv.isDirtyNoLock() {
		if !cv.canClearDirty(nt, lc.clientID) {
			lc.logger.Debugf("rimcu[%s]handleNotif: key `%s` is dirty, append notif", lc.name(), nt.Key)
			cv.appendNotif(nt)
			return
		}

		// finally got the dirty opID, need to execute the buffered notif
		lc.executeNotifs(cv, cv.getNotifBuffer()...)
		lc.executeNotifs(cv, nt)
		lc.logger.Debugf("\trimcu[%s]handleNotif: clear dirty key `%s`", lc.name(), nt.Key)
		cv.clearDirty()
		return
	}
	lc.executeNotifs(cv, nt)
}

// check if the notification can clear dirty state of this list of the given client ID
//
// TODO: maybe check for the timestampt as well
func (cv *listCacheVal) canClearDirty(nt *listNotif, clientID []byte) bool {
	if !bytes.Equal(clientID, nt.ClientID) {
		return false
	}
	return bytes.Equal(cv.getDirtyID(), nt.OpID)
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

func (lcv *listCacheVal) SetList(data []string) {
	lcv.mtx.Lock()
	lcv.list = data
	lcv.mtx.Unlock()
}

func (lcv *listCacheVal) SetDirty(opsID []byte) {
	lcv.mtx.Lock()
	defer lcv.mtx.Unlock()
	lcv.dirtyTs = time.Now().Unix()
	lcv.dirtyID = opsID
}

func (lcv *listCacheVal) ClearDirty(opID []byte) {
	lcv.mtx.Lock()
	defer lcv.mtx.Unlock()
	if !bytes.Equal(opID, lcv.getDirtyID()) {
		return
	}
	lcv.clearDirty()
}

func (lcv *listCacheVal) IsDirty() (isDirty bool) {
	lcv.mtx.RLock()
	isDirty = lcv.isDirtyNoLock()
	lcv.mtx.RUnlock()
	return
}

func (lcv *listCacheVal) isDirtyNoLock() (isDirty bool) {
	return lcv.dirtyTs != 0
}
func (lcv *listCacheVal) getDirtyID() (dirtyID []byte) {
	return lcv.dirtyID
}

func (lcv *listCacheVal) clearDirty() {
	lcv.dirtyTs = 0
	lcv.dirtyID = nil
}

func (lcv *listCacheVal) appendNotif(nt *listNotif) {
	lcv.notifBuffer = append(lcv.notifBuffer, nt)
}

func (lcv *listCacheVal) GetList() (list []string) {
	lcv.mtx.RLock()
	list = lcv.list
	lcv.mtx.RUnlock()
	return
}

func (lcv *listCacheVal) getNotifBuffer() []*listNotif {
	return lcv.notifBuffer
}

func (lcv *listCacheVal) front() (string, bool) {
	if len(lcv.list) == 0 {
		return "", false
	}
	var val string
	val, lcv.list = lcv.list[0], lcv.list[1:]
	return val, true
}

func (lcv *listCacheVal) pushBack(val string) {
	lcv.list = append(lcv.list, val)
}

const (
	listChannel  = "rimcu:list"
	listRpushCmd = "RPUSH"
	listLpopCmd  = "LPOP"
)

const (
	scriptRpush = `
redis.call('RPUSH', KEYS[1], ARGV[2])
redis.call('publish', 'rimcu:list', ARGV[1])
return 'OK'
	`
	scriptLpop = `
local val = redis.call('LPOP', KEYS[1])
redis.call('publish', 'rimcu:list', ARGV[1])
return val
	`
)
