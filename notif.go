package rimcu

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/gomodule/redigo/redis"
	"github.com/iwanbk/rimcu/internal/notif"
)

func (c *Client) runSubscriber(ctx context.Context) error {
	doneCh, err := c.startSub()
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-doneCh:
				// we're just disconnected our Notif channel,
				// clear our in mem cache as we can't assume that the values
				// still updated
				c.cc.Clear()
				doneCh, err = c.startSub()
				if err != nil {
					log.Printf("failed to start subscriber: %v", err)
				}
			}
		}
	}()
	return nil
}

func (c *Client) startSub() (chan struct{}, error) {
	doneCh := make(chan struct{})
	// setup subscriber
	conn := c.pool.Get()
	if err := conn.Err(); err != nil {
		close(doneCh)
		return doneCh, err
	}
	sub := &redis.PubSubConn{Conn: conn}

	err := sub.Subscribe(notifChannel)
	if err != nil {
		close(doneCh)
		return doneCh, err
	}

	// we're just connected to our Notif channel,
	// it means we previously not connected or disconnected from the Notif channel.
	// clear our in mem cache as we can't assume that the values
	// still updated
	c.cc.Clear()

	// run subscriber loop
	go func() {
		defer close(doneCh)
		for {
			switch v := sub.Receive().(type) {
			case redis.Message:
				err := c.handleNotif(v.Data)
				if err != nil {
					log.Printf("handleNotif failed: %v", err)
					return
				}
			case error:
				log.Printf("subscribe err: %v", v)
				return
			}
		}
	}()
	return doneCh, nil
}

// handleNotif handle raw notification from the redis
func (c *Client) handleNotif(data []byte) error {
	// decode notification
	nt, err := notif.Decode(data)
	if err != nil {
		return fmt.Errorf("failed to decode Notif:%w", err)
	}

	// ignore all updated from ourself
	if bytes.Equal(nt.ClientID, c.name) {
		return nil
	}

	fmt.Printf("[%s]msg from %s, slot:%v\n", c.name, nt.ClientID, nt.Slot)
	//c.delMemCache(nt.Key)
	c.cc.HandleNotif(nt)
	return nil
}
