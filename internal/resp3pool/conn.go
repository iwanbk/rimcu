package resp3pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/iwanbk/rimcu/logger"
	"github.com/smallnest/resp3"
)

var (
	// ErrNotFound returned when there is no cache of the given key
	ErrNotFound = errors.New("not found")

	ErrPoolExhausted = errors.New("rimcu pool exhausted")
)

// Conn is a single redis connection.
//
// It is not safe to use it concurrently
type Conn struct {
	// reader & writer for redis server communication
	w    *resp3.Writer
	rd   *resp3.Reader
	conn net.Conn

	respCh chan *resp3.Value // TODO: add resp counter

	pool   *Pool
	stopCh chan struct{}

	// callback func to call when a slot become not valid anymore.
	// it could happen in two conditions:
	// - it receives invalidation message from redis
	// - it's connection is closed, we should invalidates all slots
	invalidCb InvalidateCbFunc

	mtx     sync.Mutex
	runFlag bool

	logger logger.Logger
}

func newConn(netConn net.Conn, pool *Pool, invalidCb InvalidateCbFunc) *Conn {
	return &Conn{
		rd:        resp3.NewReader(netConn),
		w:         resp3.NewWriter(netConn),
		conn:      netConn,
		pool:      pool,
		respCh:    make(chan *resp3.Value),
		stopCh:    make(chan struct{}),
		invalidCb: invalidCb,
		logger:    pool.logger,
	}
}

func (conn *Conn) start() error {
	conn.mtx.Lock()

	if conn.runFlag { // already run
		conn.mtx.Unlock()
		return nil
	}
	conn.runFlag = true
	conn.mtx.Unlock()

	conn.run() // run in background

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := conn.do(ctx, "hello", "3")
	if err != nil {
		conn.Close()
		return err
	}
	// TODO : check result value

	_, err = conn.do(ctx, "CLIENT", "TRACKING", "ON")
	if err != nil {
		conn.Close()
		return err
	}
	// TODO : check result value

	return nil
}

// Close puts the connection back to the connection pool,
// and can still be reused later.
func (c *Conn) Close() {
	c.pool.putConnBack(c)
}

// close the connection and kill itself
func (c *Conn) closeExit() {
	c.pool.releaseConn()
	c.destroy()
}

// destroy this connection make it invalid to be used
func (c *Conn) destroy() {
	c.mtx.Lock()
	runFlag := c.runFlag
	c.mtx.Unlock()

	c.conn.Close()

	if runFlag {
		c.stopCh <- struct{}{}
	}

}

func (c *Conn) Do(ctx context.Context, cmd string, args ...string) (*resp3.Value, error) {
	cmds := append([]string{cmd}, args...)
	return c.do(ctx, cmds...)
}

func (c *Conn) do(ctx context.Context, args ...string) (*resp3.Value, error) {
	if err := c.w.WriteCommand(args...); err != nil {
		return nil, err
	}

	select {
	case val := <-c.respCh:
		return val, nil
	case <-ctx.Done():
		// if it failed, something might went wrong.
		// it is simpler to close this connection
		c.closeExit()
		return nil, ctx.Err()
	}
}

func (c *Conn) run() {
	go func() {
		defer func() {
			c.mtx.Lock()
			c.runFlag = false
			c.mtx.Unlock()
		}()
		for {
			select {
			case <-c.stopCh:
				return
			default:
			}

			// read val
			resp, _, err := c.rd.ReadValue()
			if err != nil {
				c.logger.Debugf("failed to receive a message: %v", err)
				continue
			}

			// send to respCh if not push notif
			if resp.Type != resp3.TypePush {
				//log.Printf("[non-push resp] %v:%v", resp.Type, resp.SmartResult())
				c.respCh <- resp
				continue
			}
			c.logger.Debugf("[PUSH resp] %v", resp.Type)

			// handle invalidation
			c.checkHandleInvalidation(resp)
		}
	}()
}

// check if it is invalidation message and handle it:
// - remove the tracking of this slot
// - execute the provided callback
func (c *Conn) checkHandleInvalidation(resp *resp3.Value) {
	if resp.Type != resp3.TypePush || len(resp.Elems) < 2 || resp.Elems[0].SmartResult().(string) != "invalidate" {
		return
	}

	r := resp.Elems[1]
	c.logger.Debugf("received TRACKING result: %c, %#v", resp.Type, resp.SmartResult())
	c.logger.Debugf("res[1]%c:%v", r.Type, r.Integer)

	slot := uint64(r.Integer)

	c.invalidCb(slot)

}
