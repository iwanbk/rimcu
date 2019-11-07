package resp3pool

import (
	"context"
	"net"
	"time"
)

type Pool struct {
	conns      []*Conn
	serverAddr string
}

func NewPool(serverAddr string) *Pool {
	return &Pool{
		serverAddr: serverAddr,
	}
}
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	// get from pool
	if len(p.conns) > 0 {
		var conn *Conn
		conn, p.conns = p.conns[0], p.conns[1:]
		return conn, nil
	}
	// dial
	return p.dial()
}

func (p *Pool) dial() (*Conn, error) {
	c, err := net.DialTimeout("tcp", p.serverAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return newConn(c)
}
