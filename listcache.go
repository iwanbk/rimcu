package rimcu

import (
	"container/list"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/shamaton/msgpack"
)

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

type listCacheVal struct {
	list *list.List
}
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
	if !ok || cv.list.Len() == 0 {
		return "", false
	}
	el := cv.list.Front()
	val := el.Value.(string)

	cv.list.Remove(el)
	return val, true
}

func (lc *listCache) Rpush(key, val string) {
	lc.mtx.Lock()
	defer lc.mtx.Unlock()

	cv := lc.getOrInit(key)
	cv.list.PushBack(val)
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
	cv = &listCacheVal{
		list: list.New(),
	}
	lc.cc.Add(key, cv)
	return cv
}
