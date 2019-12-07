package slotcache

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/iwanbk/rimcu/internal/crc"
	"github.com/iwanbk/rimcu/internal/notif"
)

type Cache struct {
	cc *lru.Cache
	//expNanoseconds int64

	slotMtx sync.RWMutex
	slotExp map[uint64]int64
}

type cacheVal struct {
	Slot     uint64
	TsExp    int64
	TsUpdate int64
	Val      string
}

func New(size int) (*Cache, error) {
	cc, err := lru.New(size)
	if err != nil {
		return nil, err
	}

	return &Cache{
		cc:      cc,
		slotExp: make(map[uint64]int64),
	}, nil
}

// Set sets the key Cache with the given val
func (c *Cache) SetEx(key, val string, expSecond int) {
	now := time.Now()
	v := cacheVal{
		Slot:     crc.RedisCrc([]byte(key)),
		TsUpdate: now.UnixNano(),
		TsExp:    now.Add(time.Duration(expSecond) * time.Second).UnixNano(),
		Val:      val,
	}

	c.cc.Add(key, v)
}

// Get gets the Cache val of the given key.
//
// rules:
// - check existence in Cache
// - check against expSeconds value
//		- delete if it already expired
// - check against slot expiration
//		- delete if it already expired
func (c *Cache) Get(key string) (string, bool) {
	// get value from Cache
	cv, ok := c.cc.Get(key)
	if !ok {
		return "", false
	}

	v := cv.(cacheVal)

	// check against tsExp
	if time.Now().UnixNano() > v.TsExp {
		c.cc.Remove(key)
		return "", false
	}

	// check against slot exp
	expTs, ok := c.getExp(v.Slot)
	if !ok { // not in slot expiration, it means no update from other nodes
		return v.Val, true
	}

	isExpire := expTs > v.TsUpdate
	if isExpire {
		c.cc.Remove(key)
		return "", false
	}
	return v.Val, true
}

func (c *Cache) Del(key string) {
	c.cc.Remove(key)
}

func (c *Cache) Clear() {
	c.cc.Purge()
}

func (c *Cache) HandleNotif(nt *notif.Notif) {
	c.setExp(nt.Slot, time.Now().UnixNano())
}

func (c *Cache) setExp(slot uint64, timestamp int64) {
	c.slotMtx.Lock()
	c.slotExp[slot] = timestamp
	c.slotMtx.Unlock()
}

func (c *Cache) getExp(slot uint64) (int64, bool) {
	c.slotMtx.RLock()
	val, ok := c.slotExp[slot]
	c.slotMtx.RUnlock()
	return val, ok
}

func (c *Cache) NewNotif(cliName []byte, key string) *notif.Notif {
	return &notif.Notif{
		ClientID: cliName,
		Slot:     crc.RedisCrc([]byte(key)),
	}

}
