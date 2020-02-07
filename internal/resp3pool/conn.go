package resp3pool

import (
	"errors"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/iwanbk/rimcu/internal/crc"
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

	mtx sync.Mutex
	// slots store all slots that is tracked by this connection
	slots map[uint64]struct{}

	runFlag bool
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
		slots:     make(map[uint64]struct{}),
	}
}

func (conn *Conn) start() error {
	conn.mtx.Lock()
	if conn.runFlag == true {
		conn.mtx.Unlock()
		return nil
	}
	conn.runFlag = true
	conn.mtx.Unlock()

	conn.run() // run in background

	_, err := conn.do("hello", "3")
	if err != nil {
		conn.Close()
		return err
	}
	// TODO : check result value

	_, err = conn.do("CLIENT", "TRACKING", "ON")
	if err != nil {
		conn.Close()
		return err
	}
	// TODO : check result value

	return nil
}

// Close puts the connection back to the connection pool,
// and can still be reused later
func (c *Conn) Close() {
	c.pool.putConnBack(c)
}

// destroy this connection make it invalid to be used
func (c *Conn) destroy() {
	c.mtx.Lock()
	runFlag := c.runFlag
	c.mtx.Unlock()

	if runFlag {
		c.stopCh <- struct{}{}
	}
	c.conn.Close()
}

func (c *Conn) Setex(key, val string, exp int) error {
	_, err := c.do("SET", key, val, "EX", strconv.Itoa(exp))
	return err
}

// Get value of the given key.
//
// It returns ErrNotFound if there is no cache with the given key
func (c *Conn) Get(key string) (string, error) {
	// execute redis commands
	resp, err := c.Do("GET", key)
	if err != nil {
		return "", err
	}

	if resp.Str == "" {
		// TODO : differentiate between empty string and nil value
		// ErrNotFound must only be returned on nil value
		return "", ErrNotFound
	}

	// track the slots
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.slots[crc.RedisCrc([]byte(key))] = struct{}{}

	return resp.Str, nil
}

func (c *Conn) Del(key string) error {
	// execute redis commands
	_, err := c.do("DEL", key)
	if err != nil {
		return err
	}

	// remove the slot tracking
	slot := crc.RedisCrc([]byte(key))
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.slots, slot)

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
