package rimcu

import (
	"github.com/shamaton/msgpack"
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

// listCache represents in memory cache of redis lists data type
//
// the sync mechanism is using a kind of oplog
// update listOld\
//	- update will always go to server
//	- send listNotif (cmd, key, arg)
//	- after sending update command, the in mem cache will be marked as dirty
//
// read listOld
//	- read ops always go to in mem cache except it marked as dirty
//
// dirty in mem cache
//	- in mem cache marked as dirty after it modified the respective key
//		it will be marked as clean when it receives notification of the respective operation
//	- when in dirty state, all ops of this key will be buffered
//	- the buffer will be executed when it receives notification of the respective operation
type listCache struct {
	mtx sync.RWMutex
	cc  *lru.Cache
}

func newListCache(size int) (*listCache, error) {
	cc, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &listCache{
		cc: cc,
	}, nil
}

func (lc *listCache) Lpop(key string) (string, bool) {
	lc.mtx.Lock()
	defer lc.mtx.Unlock()

	cv, ok := lc.get(key)
	if !ok {
		return "", false
	}
	return cv.Front()
}

func (lc *listCache) Rpush(key, val string) {
	lc.mtx.Lock()
	defer lc.mtx.Unlock()

	cv := lc.getOrInit(key)
	cv.PushBack(val)
}

func (lc *listCache) get(key string) (*listCacheVal, bool) {
	entry, ok := lc.cc.Get(key)
	if !ok {
		return nil, false
	}
	return entry.(*listCacheVal), true
}

func (lc *listCache) getOrInit(key string) *listCacheVal {
	cv, ok := lc.get(key)
	if ok {
		return cv
	}
	cv = &listCacheVal{}
	lc.cc.Add(key, cv)
	return cv
}

// listCacheVal represents in memory backing of the listCache
type listCacheVal struct {
	list []string
}

func (lcv *listCacheVal) Len() int {
	return len(lcv.list)
}

func (lcv *listCacheVal) Front() (string, bool) {
	if lcv.Len() == 0 {
		return "", false
	}
	var val string
	val, lcv.list = lcv.list[0], lcv.list[1:]
	return val, true
}

func (lcv *listCacheVal) PushBack(val string) {
	lcv.list = append(lcv.list, val)
}

// listNotif represents notification of the list write operation
type listNotif struct {
	ClientID []byte
	Cmd      string
	Key      string
	Arg      string
}

func (ln *listNotif) Encode() ([]byte, error) {
	return msgpack.Encode(ln)
}

func decodeListNotif(b []byte) (*listNotif, error) {
	var ln listNotif
	return &ln, msgpack.Decode(b, &ln)
}
