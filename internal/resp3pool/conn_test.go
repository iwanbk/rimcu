package resp3pool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConn(t *testing.T) {
	ctx := context.Background()

	pool := NewPool("localhost:6379")
	c1, err := pool.Get(ctx)
	require.NoError(t, err)

	const (
		key1 = "key_1"
		val1 = "val_1"
		//val2 = "val_2"
	)
	err = c1.Set(key1, val1)
	require.NoError(t, err)
}
