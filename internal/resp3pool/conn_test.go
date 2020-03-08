package resp3pool

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/iwanbk/rimcu/internal/crc"
	"github.com/stretchr/testify/require"
)

func TestConn(t *testing.T) {
	var (
		c2InvalidationCh = make(chan struct{})
		ctx              = context.Background()
		exp              = 1000
	)
	pool1 := NewPool(PoolConfig{
		ServerAddr: "localhost:6379",
		InvalidateCb: func(slot uint64) {
			log.Printf("invalidate callback %v", slot)
		},
	})
	pool2 := NewPool(PoolConfig{
		ServerAddr: "localhost:6379",
		InvalidateCb: func(slot uint64) {
			c2InvalidationCh <- struct{}{}
		},
	})

	c1, err := pool1.Get(ctx)
	require.NoError(t, err)
	defer c1.Close()

	c2, err := pool2.Get(ctx)
	require.NoError(t, err)
	defer c2.Close()

	const (
		key1 = "key_1"
		val1 = "val_1"
		val2 = "val_2"
	)
	log.Printf("key crc = %v", crc.RedisCrc([]byte(key1)))

	err = c1.setex(key1, val1, exp)
	require.NoError(t, err)

	val, err := c2.get(key1)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	err = c1.setex(key1, val2, exp)
	require.NoError(t, err)

	select {
	case <-c2InvalidationCh:
	case <-time.After(5 * time.Second):
		t.Errorf("don't receive invalidation after 5 seconds")
	}
}

func (c *Conn) setex(key, val string, exp int) error {
	_, err := c.do(context.Background(), "SET", key, val, "EX", strconv.Itoa(exp))
	return err
}

// get value of the given key.
//
// It returns ErrNotFound if there is no cache with the given key
func (c *Conn) get(key string) (string, error) {
	// execute redis commands
	resp, err := c.Do(context.Background(), "GET", key)
	if err != nil {
		return "", err
	}

	if resp.Str == "" {
		// TODO : differentiate between empty string and nil value
		// ErrNotFound must only be returned on nil value
		return "", ErrNotFound
	}

	return resp.Str, nil
}
