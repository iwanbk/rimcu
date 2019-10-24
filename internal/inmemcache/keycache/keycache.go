package keycache

import (
	"time"

	"github.com/iwanbk/rimcu/internal/notif"
	"github.com/karlseguin/ccache"
)

type KeyCache struct {
	cc *ccache.Cache
}

func New(maxSize int) *KeyCache {
	return &KeyCache{
		cc: ccache.New(ccache.Configure().MaxSize(int64(maxSize))),
	}
}

func (kc *KeyCache) SetEx(key, val string, expSecond int) {
	kc.cc.Set(key, val, time.Second*time.Duration(expSecond))
}

func (kc *KeyCache) Get(key string) (string, bool) {
	item := kc.cc.Get(key)
	if item == nil || item.Expired() {
		return "", false
	}
	return item.Value().(string), true
}

func (kc *KeyCache) Del(key string) {
	kc.cc.Delete(key)
}
func (kc *KeyCache) Clear() {
	kc.cc.Clear()
}

func (kc *KeyCache) HandleNotif(nt *notif.Notif) {
	kc.cc.Delete(nt.Key)
}

func (kc *KeyCache) NewNotif(cliName []byte, key string) *notif.Notif {
	return &notif.Notif{
		ClientID: cliName,
		Key:      key,
	}

}
