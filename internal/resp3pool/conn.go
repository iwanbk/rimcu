package resp3pool

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/iwanbk/rimcu/internal/crc"
	"github.com/smallnest/resp3"
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

	mtx sync.Mutex
	// slots store all slots that is tracked by this connection
	slots map[uint64]struct{}
}

func newConn(netConn net.Conn, pool *Pool, invalidCb InvalidateCbFunc) (*Conn, error) {
	conn := &Conn{
		rd:        resp3.NewReader(netConn),
		w:         resp3.NewWriter(netConn),
		conn:      netConn,
		pool:      pool,
		respCh:    make(chan *resp3.Value),
		stopCh:    make(chan struct{}),
		invalidCb: invalidCb,
		slots:     make(map[uint64]struct{}),
	}
	conn.run() // run in background

	_, err := conn.do("hello", "3")
	if err != nil {
		conn.Close()
		return nil, err
	}
	// TODO : check result value

	_, err = conn.do("CLIENT", "TRACKING", "ON")
	if err != nil {
		conn.Close()
		return nil, err
	}
	// TODO : check result value

	return conn, nil
}

// Close puts the connection back to the connection pool,
// and can still be reused later
func (c *Conn) Close() {
	c.pool.putConnBack(c)
}

// destroy this connection make it invalid to be used
func (c *Conn) destroy() {
	c.conn.Close()
	c.stopCh <- struct{}{}
}

func (c *Conn) Setex(key, val string, exp int) error {
	_, err := c.do("SET", key, val, "EX", strconv.Itoa(exp))
	return err
}

func (c *Conn) Get(key string) (string, error) {
	// execute redis commands
	resp, err := c.Do("GET", key)
	if err != nil {
		return "", err
	}

	// track the slots
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.slots[crc.RedisCrc([]byte(key))] = struct{}{}

	return resp.Str, nil
}

func (c *Conn) Ping() error {
	resp, err := c.do([]string{"PING"}...)
	if err != nil {
		return err
	}
	if resp.Str != "PONG" {
		return fmt.Errorf("invalid PING reply:%v", resp.Str)
	}
	return nil
}

func (c *Conn) Do(cmd, key string, args ...string) (*resp3.Value, error) {
	cmds := append([]string{cmd, key}, args...)
	return c.do(cmds...)
}

func (c *Conn) do(args ...string) (*resp3.Value, error) {
	if err := c.w.WriteCommand(args...); err != nil {
		return nil, err
	}

	val := <-c.respCh // TODO: add some timeout mechanism
	return val, nil
}

func (c *Conn) run() {
	go func() {
		for {
			select {
			case <-c.stopCh:
				return
			default:
			}

			// read val
			resp, _, err := c.rd.ReadValue()
			if err != nil {
				log.Printf("failed to receive a message: %v", err)
				continue
			}

			// send to respCh if not push notif
			if resp.Type != resp3.TypePush {
				//log.Printf("[non-push resp] %v:%v", resp.Type, resp.SmartResult())
				c.respCh <- resp
				continue
			}
			log.Printf("[PUSH resp] %v", resp.Type)

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
	log.Printf("received TRACKING result: %c, %#v", resp.Type, resp.SmartResult())
	log.Printf("res[1]%c:%v", r.Type, r.Integer)

	slot := uint64(r.Integer)

	c.mtx.Lock()
	delete(c.slots, slot)
	c.mtx.Unlock()

	c.invalidCb(slot)

}
