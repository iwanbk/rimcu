package rimcu

import (
	"context"
	"sync"
	"time"

	"github.com/iwanbk/rimcu/internal/crc"
	"github.com/iwanbk/rimcu/internal/resp3pool"
	"github.com/karlseguin/ccache"
)

type StringsCacheResp3 struct {
	pool *resp3pool.Pool
	cc   *ccache.Cache

	mtx   sync.Mutex
	slots map[uint64]slotEntries
}

type StringsCacheResp3Config struct {
	ServerAddr string
	CacheSize  int
}

func NewStringsCacheResp3(cfg StringsCacheResp3Config) *StringsCacheResp3 {
	return &StringsCacheResp3{
		pool:  resp3pool.NewPool(cfg.ServerAddr),
		cc:    ccache.New(ccache.Configure().MaxSize(1000)),
		slots: make(map[uint64]slotEntries),
	}
}

func (scr *StringsCacheResp3) Setex(ctx context.Context, key, val string, exp int) error {
	conn, err := scr.pool.Get(ctx, scr.invalidate)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Setex(key, val, exp)
}

func (scr *StringsCacheResp3) Get(ctx context.Context, key string, exp int) (string, error) {
	// get from mem, if exists
	val, ok := scr.memGet(key)
	if ok {
		return val, nil
	}

	// get fom redis
	conn, err := scr.pool.Get(ctx, scr.invalidate)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	val, err = conn.Get(key)
	if err != nil {
		return "", err
	}

	// add to in mem cache
	scr.memSet(key, val, time.Duration(exp)*time.Second)

	return val, nil
}

// memSet sets the value of the given key.
//
// it also add the key to the slots map
func (scr *StringsCacheResp3) memSet(key, val string, exp time.Duration) {
	// add in cache
	scr.cc.Set(key, val, exp)

	// track
	var (
		se   slotEntries
		ok   bool
		slot = crc.RedisCrc([]byte(key))
	)

	scr.mtx.Lock()
	se, ok = scr.slots[slot]
	if !ok {
		se = makeSlotEntries()
	}
	se.add(key)
	scr.slots[slot] = se
	scr.mtx.Unlock()
}

func (scr *StringsCacheResp3) memGet(key string) (string, bool) {
	item := scr.cc.Get(key)
	if item == nil {
		return "", false
	}
	if item.Expired() {
		return "", false
	}
	return item.Value().(string), true
}

func (scr *StringsCacheResp3) invalidate(slot uint64) {
	se, ok := scr.slots[slot]
	if !ok {
		return
	}

	scr.mtx.Lock()
	delete(scr.slots, slot)
	scr.mtx.Unlock()

	for key := range se {
		scr.cc.Delete(key)
	}
}

// slot entries
type slotEntries map[string]struct{}

func makeSlotEntries() slotEntries {
	se := make(map[string]struct{})
	return slotEntries(se)
}

func (se slotEntries) add(key string) {
	se[key] = struct{}{}
}

func (se slotEntries) get(key string) bool {
	_, ok := se[key]
	return ok
}
