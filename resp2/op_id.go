package resp2

import (
	"fmt"
	"sync"

	"github.com/rs/xid"
)

type opIDGenerator interface {
	Generate() []byte
}
type defaultOpIDgen struct {
}

func (d *defaultOpIDgen) Generate() []byte {
	return xid.New().Bytes()
}

type testOpIDGen struct {
	count int
	mtx   sync.Mutex
}

func (t *testOpIDGen) Generate() []byte {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.count++

	return []byte(fmt.Sprintf("op_%d", t.count))
}
