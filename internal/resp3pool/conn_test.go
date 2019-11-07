package resp3pool

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConn(t *testing.T) {
	ctx := context.Background()

	pool := NewPool("localhost:6379")

	c1, err := pool.Get(ctx)
	require.NoError(t, err)

	c2, err := pool.Get(ctx)
	require.NoError(t, err)

	const (
		key1 = "key_1"
		val1 = "val_1"
		val2 = "val_2"
	)
	log.Printf("key crc = %v", redisCrc([]byte(key1)))

	err = c1.Set(key1, val1)
	require.NoError(t, err)

	val, err := c2.Get(key1)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	err = c1.Set(key1, val2)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
}
