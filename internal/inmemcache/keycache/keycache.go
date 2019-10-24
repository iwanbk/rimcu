package keycache

import (
	"time"

	"github.com/hashicorp/golang-lru"
	"github.com/iwanbk/rimcu/internal/notif"
)

type KeyCache struct {
	cc *lru.Cache
}
type cacheVal struct {
	TsExp int64
	Val   string
}

func New(maxSize int) (*KeyCache, error) {
	cc, err := lru.New(maxSize)
	if err != nil {
		return nil, err
	}
	return &KeyCache{
		cc: cc,
	}, nil
}

func (kc *KeyCache) SetEx(key, val string, expSecond int) {

	v := cacheVal{
		TsExp: time.Now().Add(time.Duration(expSecond) * time.Second).UnixNano(),
		Val:   val,
	}

	kc.cc.Add(key, v)
}

func (kc *KeyCache) Get(key string) (string, bool) {
	// get value from Cache
	cv, ok := kc.cc.Get(key)
	if !ok {
		return "", false
	}

	v := cv.(cacheVal)

	// check against tsExp
	if time.Now().UnixNano() > v.TsExp {
		kc.cc.Remove(key)
		return "", false
	}
	return v.Val, true
}

func (kc *KeyCache) Del(key string) {
	kc.cc.Remove(key)
}
func (kc *KeyCache) Clear() {
	kc.cc.Purge()
}

func (kc *KeyCache) HandleNotif(nt *notif.Notif) {
	kc.Del(nt.Key)
}

func (kc *KeyCache) NewNotif(cliName []byte, key string) *notif.Notif {
	return &notif.Notif{
		ClientID: cliName,
		Key:      key,
	}
}
