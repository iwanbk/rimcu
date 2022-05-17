package rimcu

import (
	"github.com/iwanbk/rimcu/logger"
)

// Protocol represents the underlying Redis protocol used by rimcu.
// It currently support RESP2 & RESP3 (experimental)
type Protocol string

const (
	// ProtoResp2 represent RESP2 protocol that is used from Redis 2
	ProtoResp2 Protocol = "RESP2"

	// ProtoResp3 represents RESP3 protocol that supported since Redis 6
	ProtoResp3 Protocol = "RESP3"
)

// Config represents config of Cache
type Config struct {
	// redis server address
	ServerAddr string

	// size of the  in memory cache
	// Default is 10K
	CacheSize int

	// Protocol of redis being used.
	//
	// The default is ProtoResp2
	Protocol Protocol

	// logger to be used, use default logger which print to stderr on error
	Logger logger.Logger
}

// Rimcu is a redis client which implements client side caching.
// It is safe for concurrent use by multiples goroutine
type Rimcu struct {
	serverAddr string
	logger     logger.Logger
	protocol   Protocol
}

// New creates a new Rimcu redis client
func New(cfg Config) *Rimcu {
	return &Rimcu{
		serverAddr: cfg.ServerAddr,
		logger:     cfg.Logger,
		protocol:   cfg.Protocol,
	}
}

// NewStringsCache creates a new strings cache and do the required initialization
func (r *Rimcu) NewStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {
	cfg.logger = r.logger
	cfg.serverAddr = r.serverAddr
	return newStringsCache(cfg)
}

const (
	defaultCacheSize   = 100000
	defaultCacheTTLSec = 60 * 20
)
