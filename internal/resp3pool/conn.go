package resp3pool

import (
	"fmt"
	"log"
	"net"

	"github.com/smallnest/resp3"
)

type Conn struct {
	w      *resp3.Writer
	rd     *resp3.Reader
	respCh chan *resp3.Value // TODO: add resp counter
	stopCh chan struct{}
}

func newConn(netConn net.Conn) (*Conn, error) {
	conn := &Conn{
		rd:     resp3.NewReader(netConn),
		w:      resp3.NewWriter(netConn),
		respCh: make(chan *resp3.Value),
		stopCh: make(chan struct{}),
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

func (c *Conn) Set(key, val string) error {
	_, err := c.Do("SET", key, val)
	return err
}

func (c *Conn) Get(key string) (string, error) {
	resp, err := c.Do("GET", key)
	if err != nil {
		return "", err
	}
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
func (c *Conn) Close() {
	c.stopCh <- struct{}{}
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
			if len(resp.Elems) >= 2 && resp.Elems[0].SmartResult().(string) == "invalidate" {
				log.Printf("received TRACKING result: %c, %+v", resp.Type, resp.SmartResult())
				//res, ok := resp.Elems[0].SmartResult()
				// refresh or delete the cache
			}
		}
	}()
}
