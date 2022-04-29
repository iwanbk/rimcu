package rimcu

import (
	"context"
	"github.com/iwanbk/rimcu/logger"
	"github.com/iwanbk/rimcu/resp2"
	"github.com/iwanbk/rimcu/resp3"
	"github.com/iwanbk/rimcu/result"
)

type StringsCache struct {
	engine stringsCacheEngine
}

type stringsCacheEngine interface {
	Get(ctx context.Context, key string, expSecond int) (result.StringsResult, error)
}

type StringsCacheConfig struct {
	CacheSize  int
	CacheTTL   int
	protocol   Protocol
	serverAddr string
	logger     logger.Logger
}

func newStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {
	var (
		engine stringsCacheEngine
		err    error
	)
	if cfg.protocol == ProtoResp3 {
		engine = resp3.New(resp3.Config{
			ServerAddr: cfg.serverAddr,
			Logger:     cfg.logger,
		})
	} else {
		engine, err = resp2.NewStringsCache(resp2.StringsCacheConfig{
			ServerAddr: cfg.serverAddr,
			Logger:     cfg.logger,
		})
	}
	if err != nil {
		return nil, err
	}

	return &StringsCache{
		engine: engine,
	}, nil
}

func (sc *StringsCache) Get(ctx context.Context, key string, expSecond int) (result.StringsResult, error) {
	return sc.engine.Get(ctx, key, expSecond)
}
