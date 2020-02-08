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

	// channel which acts like a semaphore for the pool's number of connections
	// - up/added when we create new connection
	// - down/removed when: the conn returned to the pool
	maxConnsCh chan struct{}
}

type PoolConfig struct {
	ServerAddr   string
	MaxConns     int // default:50
	InvalidateCb InvalidateCbFunc
}

// NewPool creates new connection pool from the given server address
func NewPool(cfg PoolConfig) *Pool {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 50
	}
	return &Pool{
		serverAddr:   cfg.ServerAddr,
		invalidateCb: cfg.InvalidateCb,
		maxConnsCh:   make(chan struct{}, cfg.MaxConns),
	}
}

type InvalidateCbFunc func(uint64)

// Get connections from the pool or create a new one.
//
// ctx is context being used to wait when the pool is exhausted.
// it should have timeout to avoid waiting indefinitely
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	conn, err := p.get(ctx)
	if err != nil {
		return nil, err
	}
	return conn, conn.start()
}

func (p *Pool) get(ctx context.Context) (*Conn, error) {
	select {
	case p.maxConnsCh <- struct{}{}:
	case <-ctx.Done():
		return nil, ErrPoolExhausted
	}

	// get from pool
	conn, ok := p.getConnFromPool()
	if ok {
		return conn, nil
	}

	// dial
	conn, err := p.dial(p.invalidateCb)
	if err != nil {
		<-p.maxConnsCh
		return nil, err
	}
	return conn, nil
}

// get existing connection from front of the pool
// TODO:
// - add some health checking
// - add idle timeout checking
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
	return newConn(c, p, invalidCb), nil
}

// putConnBack put the connection back to the the end of the pool
func (p *Pool) putConnBack(conn *Conn) {
	p.mtx.Lock()
	p.conns = append(p.conns, conn)
	p.mtx.Unlock()
	<-p.maxConnsCh
}

func (p *Pool) Close() {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, conn := range p.conns {
		conn.destroy()
	}
}
