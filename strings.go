package rimcu

import (
	"context"
	"fmt"

	"github.com/iwanbk/rimcu/logger"
	"github.com/iwanbk/rimcu/resp2"
	"github.com/iwanbk/rimcu/resp3"
	"github.com/iwanbk/rimcu/result"
)

// StringsCache is Rimcu client for the strings redis data type
type StringsCache struct {
	engine stringsCacheEngine
}

type stringsCacheEngine interface {
	Setex(ctx context.Context, key string, val interface{}, exp int) error
	Get(ctx context.Context, key string, expSecond int) (result.StringsResult, error)
	Del(ctx context.Context, key string) error
}

// StringsCacheConfig is the configuration of the StringsCache
type StringsCacheConfig struct {
	CacheSize    int
	CacheTTLSec  int
	protocol     Protocol
	serverAddr   string
	logger       logger.Logger
	clusterNodes []string // TODO: make it only 1 listener
	password     string
	dataPool     resp2.DataPool
}

func newStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {
	var (
		engine stringsCacheEngine
		err    error
	)
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = defaultCacheSize
	}
	if cfg.CacheTTLSec <= 0 {
		cfg.CacheTTLSec = defaultCacheTTLSec
	}

	switch cfg.protocol {
	case ProtoResp3:
		engine = resp3.New(resp3.Config{
			ServerAddr: cfg.serverAddr,
			Logger:     cfg.logger,
		})
	case ProtoResp2, ProtoResp2ClusterProxy:
		var mode = resp2.ModeSingle
		if cfg.protocol == ProtoResp2ClusterProxy {
			mode = resp2.ModeClusterProxy
		}
		engine, err = resp2.NewStringsCache(resp2.StringsCacheConfig{
			CacheSize:    cfg.CacheSize,
			ServerAddr:   cfg.serverAddr,
			Logger:       cfg.logger,
			ClusterNodes: cfg.clusterNodes,
			Password:     cfg.password,
			Mode:         mode,
			DataPool:     cfg.dataPool,
		})
	default:
		err = fmt.Errorf("unknown protocol: %s", cfg.protocol)
	}

	if err != nil {
		return nil, err
	}

	return &StringsCache{
		engine: engine,
	}, nil
}

// Setex sets the key to hold the string value with the given expiration second.
//
// Calling this func will invalidate inmem cache of this key's slot in all nodes
func (sc *StringsCache) Setex(ctx context.Context, key string, val interface{}, exp int) error {
	return sc.engine.Setex(ctx, key, val, exp)
}

// Get gets the value of key.
//
// It gets from the redis server only if the value not exists in memory cache,
// it then put the value from server in the in memcache with the given expiration
func (sc *StringsCache) Get(ctx context.Context, key string, expSecond int) (result.StringsResult, error) {
	return sc.engine.Get(ctx, key, expSecond)
}

// Del deletes the key in both memory cache and redis server
func (sc *StringsCache) Del(ctx context.Context, key string) error {
	return sc.engine.Del(ctx, key)
}
