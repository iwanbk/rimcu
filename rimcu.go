package rimcu

import (
	"github.com/iwanbk/rimcu/logger"
)

type Protocol string

const (
	ProtoResp2 Protocol = "RESP2"
	ProtoResp3 Protocol = "RESP3"
)

// Config represents config of Cache
type Config struct {
	// redis server address
	ServerAddr string

	// size of the  in memory cache
	// Default is 10K
	CacheSize int

	Protocol Protocol

	// logger to be used, use default logger which print to stderr on error
	Logger logger.Logger
}

type Rimcu struct {
	serverAddr string
	logger     logger.Logger
}

func New(cfg Config) *Rimcu {
	return &Rimcu{
		logger: cfg.Logger,
	}
}

func (r *Rimcu) NewStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {
	cfg.logger = r.logger
	cfg.serverAddr = r.serverAddr
	return newStringsCache(cfg)
}
