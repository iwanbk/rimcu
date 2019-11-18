package resp3pool

import (
	"context"
	"net"
	"sync"
	"time"
)

// Pool represents a pool of connection.
//
// It is currently very simple untested pool
type Pool struct {
	serverAddr   string
	invalidateCb InvalidateCbFunc

	mtx   sync.Mutex
	conns []*Conn
}

type PoolConfig struct {
	ServerAddr   string
	InvalidateCb InvalidateCbFunc
}

// NewPool creates new connection pool from the given server address
func NewPool(cfg PoolConfig) *Pool {
	return &Pool{
		serverAddr:   cfg.ServerAddr,
		invalidateCb: cfg.InvalidateCb,
	}
}

type InvalidateCbFunc func(uint64)

func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	// get from pool
	conn, ok := p.getConnFromPool()
	if ok {
		return conn, nil
	}

	// dial
	return p.dial(p.invalidateCb)
}

// get existing connection from front of the pool
// TODO: add some healtch checking
func (p *Pool) getConnFromPool() (*Conn, bool) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if len(p.conns) == 0 {
		return nil, false
	}

	var conn *Conn
	conn, p.conns = p.conns[0], p.conns[1:]

	return conn, true
}

// dial create new connection
func (p *Pool) dial(invalidCb InvalidateCbFunc) (*Conn, error) {
	c, err := net.DialTimeout("tcp", p.serverAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return newConn(c, p, invalidCb)
}

// putConnBack put the connection back to the the end of the pool
func (p *Pool) putConnBack(conn *Conn) {
	p.mtx.Lock()
	p.conns = append(p.conns, conn)
	p.mtx.Unlock()
}
