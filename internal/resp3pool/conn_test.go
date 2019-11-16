package resp3pool

import (
	"context"
	"github.com/iwanbk/rimcu/internal/crc"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConn(t *testing.T) {
	var (
		pool1            = NewPool("localhost:6379")
		pool2            = NewPool("localhost:6379")
		c2InvalidationCh = make(chan struct{})
		ctx              = context.Background()
		exp              = int64(1000)
	)

	c1, err := pool1.Get(ctx, func(slot uint64) {
		log.Printf("invalidate callback %v", slot)
	})
	require.NoError(t, err)
	defer c1.Close()

	c2, err := pool2.Get(ctx, func(slot uint64) {
		c2InvalidationCh <- struct{}{}
	})
	require.NoError(t, err)
	defer c2.Close()

	const (
		key1 = "key_1"
		val1 = "val_1"
		val2 = "val_2"
	)
	log.Printf("key crc = %v", crc.RedisCrc([]byte(key1)))

	err = c1.Setex(key1, val1, exp)
	require.NoError(t, err)

	val, err := c2.Get(key1)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	err = c1.Setex(key1, val2, exp)
	require.NoError(t, err)

	select {
	case <-c2InvalidationCh:
	case <-time.After(5 * time.Second):
		t.Errorf("don't receive invalidation after 5 seconds")
	}
}
