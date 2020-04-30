package resp2

import "sync"

// connKeyMap stores info about keys that
// associated with client/conn ID
type connKeyMap struct {
	mtx sync.Mutex
	m   map[int64]*keysMap
}

func newConnKeyMap() *connKeyMap {
	return &connKeyMap{
		m: make(map[int64]*keysMap),
	}
}

// adds key to the given client ID
func (ckm *connKeyMap) add(clientID int64, key string) {
	ckm.mtx.Lock()
	defer ckm.mtx.Unlock()

	km, ok := ckm.m[clientID]
	if !ok {
		km = newKeysMap()
	}
	km.add(key)
	ckm.m[clientID] = km
}

func (ckm *connKeyMap) del(clientID int64, key string) {
	ckm.mtx.Lock()
	defer ckm.mtx.Unlock()

	km, ok := ckm.m[clientID]
	if !ok {
		// TODO: give some warning
		return
	}
	km.del(key)

}

func (ckm *connKeyMap) clean(clientID int64) {
	ckm.mtx.Lock()
	defer ckm.mtx.Unlock()

	delete(ckm.m, clientID)
}

// keys returns all keys associated with a client ID
func (ckm *connKeyMap) keys(clientID int64) map[string]struct{} {
	ckm.mtx.Lock()
	defer ckm.mtx.Unlock()

	km, ok := ckm.m[clientID]
	if !ok {
		return make(map[string]struct{})
	}
	return km.m
}

// keysMap is map of keys that hold by a client connection
type keysMap struct {
	m map[string]struct{}
}

func newKeysMap() *keysMap {
	return &keysMap{
		m: make(map[string]struct{}),
	}
}

func (km *keysMap) add(key string) {
	km.m[key] = struct{}{}
}

func (km *keysMap) del(key string) {
	delete(km.m, key)
}
