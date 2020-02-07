package resp3pool

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/require"
)

// Test max conns property of the connection pool
func TestPool_Max(t *testing.T) {
	srv, err := miniredis.Run()
	require.NoError(t, err)
	defer srv.Close()

	var (
		maxConns  = 10
		firstConn *Conn
	)

	pool := NewPool(PoolConfig{
		ServerAddr:   srv.Addr(),
		MaxConns:     maxConns,
		InvalidateCb: nil,
	})
	defer pool.Close()

	// get conn until max
	for i := 0; i < maxConns; i++ {
		conn, err := pool.get(context.Background())
		require.NoError(t, err)
		if i == 0 {
			firstConn = conn
		} else {
			defer conn.Close() // only close in the end of the test
		}
	}

	// next call must fail
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = pool.get(ctx)
	require.Equal(t, ErrPoolExhausted, err)

	// close one of the conn, and get again from the pool
	firstConn.Close()

	conn, err := pool.get(context.Background())
	require.NoError(t, err)
	conn.Close()
}
