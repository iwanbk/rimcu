package rimcu

import (
	"context"
	"sync"
	"time"

	"github.com/iwanbk/rimcu/internal/crc"
	"github.com/iwanbk/rimcu/internal/resp3pool"
	"github.com/karlseguin/ccache"
)

// StringsCacheResp3 represents strings cache with redis RESP3 protocol
type StringsCacheResp3 struct {
	pool  *resp3pool.Pool
	cc    *ccache.Cache
	slots *slot
}

// StringsCacheResp3Config represents config of strings cache with redis RESP3 protocol
type StringsCacheResp3Config struct {
	// redis server address
	ServerAddr string

	// size of the  in memory cache
	CacheSize int
}

// NewStringsCacheResp3 create strings cache with redis RESP3 protocol
func NewStringsCacheResp3(cfg StringsCacheResp3Config) *StringsCacheResp3 {
	sc := &StringsCacheResp3{
		cc:    ccache.New(ccache.Configure().MaxSize(1000)),
		slots: newSlot(),
	}
	poolCfg := resp3pool.PoolConfig{
		ServerAddr:   cfg.ServerAddr,
		InvalidateCb: sc.invalidate,
	}
	sc.pool = resp3pool.NewPool(poolCfg)
	return sc
}

// Setex sets the key to hold the string value with the given expiration second
func (scr *StringsCacheResp3) Setex(ctx context.Context, key, val string, exp int64) error {
	conn, err := scr.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Setex(key, val, exp)
}

// Get gets the value of key.
//
// It gets from the redis server only if the value not exists in memory cache,
// it then put the value from server in the in memcache with the given expiration
func (scr *StringsCacheResp3) Get(ctx context.Context, key string, exp int64) (string, error) {
	// get from mem, if exists
	val, ok := scr.memGet(key)
	if ok {
		return val, nil
	}

	// get fom redis
	conn, err := scr.pool.Get(ctx)
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

func (scr *StringsCacheResp3) Del(ctx context.Context, key string) error {
	conn, err := scr.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = conn.Del(key)
	if err != nil {
		return err
	}
	scr.memDel(key)
	return nil
}

// memSet sets the value of the given key.
//
// it also add the key to the slots map
func (scr *StringsCacheResp3) memSet(key, val string, exp time.Duration) {
	// add in cache
	scr.cc.Set(key, val, exp)

	// add this key in the slots
	scr.slots.addKey(key)
}

func (scr *StringsCacheResp3) memDel(key string) {
	scr.cc.Delete(key)
	scr.slots.removeKey(key)
}

func (scr *StringsCacheResp3) memGet(key string) (string, bool) {
	item := scr.cc.Get(key)
	if item == nil {
		return "", false
	}
	if item.Expired() {
		scr.cc.Delete(key)
		return "", false
	}

	return item.Value().(string), true
}

// invalidate the given slot
func (scr *StringsCacheResp3) invalidate(slot uint64) {
	se := scr.slots.removeSlot(slot)
	// delete all the keys from the memory cache
	for key := range se {
		scr.cc.Delete(key)
	}
}

type slot struct {
	mtx   sync.Mutex
	slots map[uint64]slotEntries
}

func newSlot() *slot {
	return &slot{
		slots: make(map[uint64]slotEntries),
	}
}

func (s *slot) addKey(key string) {
	var (
		slotKey = crc.RedisCrc([]byte(key))
		se      slotEntries
		ok      bool
	)

	s.mtx.Lock()
	defer s.mtx.Unlock()

	se, ok = s.slots[slotKey]
	if !ok {
		se = makeSlotEntries()
	}
	se.add(key)
	s.slots[slotKey] = se
}

func (s *slot) removeKey(key string) {
	var (
		slotKey = crc.RedisCrc([]byte(key))
	)
	s.mtx.Lock()
	defer s.mtx.Unlock()

	se, ok := s.slots[slotKey]
	if !ok {
		return
	}
	delete(se, key)
	s.slots[slotKey] = se
}

func (s *slot) removeSlot(slotKey uint64) map[string]struct{} {
	s.mtx.Lock()
	se, ok := s.slots[slotKey]
	if !ok {
		s.mtx.Unlock()
		return nil
	}
	delete(s.slots, slotKey)

	s.mtx.Unlock()
	return se
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
