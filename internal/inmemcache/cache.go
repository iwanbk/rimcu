package inmemcache

import (
	"github.com/iwanbk/rimcu/internal/notif"
)

type Cache interface {
	// Set key to the given value
	SetEx(key, val string, expSecond int)

	// Get value of the given key
	Get(key string) (string, bool)

	// Del cache of the given key
	Del(key string)

	// Clear all the cache
	Clear()

	HandleNotif(*notif.Notif)

	NewNotif(cliName []byte, key string) *notif.Notif
}
