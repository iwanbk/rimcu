package rimcu

import (
	"context"
	"sync"
	"time"

	"github.com/iwanbk/rimcu/internal/crc"
	"github.com/iwanbk/rimcu/internal/resp3pool"
	"github.com/karlseguin/ccache"
)

// StringsCache represents strings cache with redis RESP3 protocol
type StringsCache struct {
	pool  *resp3pool.Pool
	cc    *ccache.Cache
	slots *slot
}

// StringsCacheConfig represents config of strings cache with redis RESP3 protocol
type StringsCacheConfig struct {
	// redis server address
	ServerAddr string

	// size of the  in memory cache
	CacheSize int
}

// NewStringsCache create strings cache with redis RESP3 protocol
func NewStringsCache(cfg StringsCacheConfig) *StringsCache {
	sc := &StringsCache{
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
func (sc *StringsCache) Setex(ctx context.Context, key, val string, exp int64) error {
	conn, err := sc.pool.Get(ctx)
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
func (sc *StringsCache) Get(ctx context.Context, key string, exp int64) (string, error) {
	// get from mem, if exists
	val, ok := sc.memGet(key)
	if ok {
		return val, nil
	}

	// get fom redis
	conn, err := sc.pool.Get(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	val, err = conn.Get(key)
	if err != nil {
		return "", err
	}

	// add to in mem cache
	sc.memSet(key, val, time.Duration(exp)*time.Second)

	return val, nil
}

// Del deletes the key in local and remote
func (sc *StringsCache) Del(ctx context.Context, key string) error {
	conn, err := sc.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = conn.Del(key)
	if err != nil {
		return err
	}
	sc.memDel(key)
	return nil
}

// memSet sets the value of the given key.
//
// it also add the key to the slots map
func (sc *StringsCache) memSet(key, val string, exp time.Duration) {
	// add in cache
	sc.cc.Set(key, val, exp)

	// add this key in the slots
	sc.slots.addKey(key)
}

func (sc *StringsCache) memDel(key string) {
	sc.cc.Delete(key)
	sc.slots.removeKey(key)
}

func (sc *StringsCache) memGet(key string) (string, bool) {
	item := sc.cc.Get(key)
	if item == nil {
		return "", false
	}
	if item.Expired() {
		sc.cc.Delete(key)
		return "", false
	}

	return item.Value().(string), true
}

// invalidate the given slot
func (sc *StringsCache) invalidate(slot uint64) {
	se := sc.slots.removeSlot(slot)
	// delete all the keys from the memory cache
	for key := range se {
		sc.cc.Delete(key)
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
