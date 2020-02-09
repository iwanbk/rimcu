package rimcu

import (
	"sync"

	"github.com/iwanbk/rimcu/internal/crc"
)

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
