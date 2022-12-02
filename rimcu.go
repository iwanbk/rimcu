package rimcu

import (
	"github.com/iwanbk/rimcu/logger"
	"github.com/iwanbk/rimcu/resp2"
)

// Protocol represents the underlying Redis protocol used by rimcu.
// It currently support RESP2 & RESP3 (experimental)
type Protocol string

const (
	// ProtoResp2 represent RESP2 protocol that is used from Redis 2
	ProtoResp2 Protocol = "RESP2"

	// ProtoResp2ClusterProxy represent RESP2 protocol on redis cluster
	// with front proxy
	ProtoResp2ClusterProxy Protocol = "RESP2ClusterProxy"

	// ProtoResp3 represents RESP3 protocol that supported since Redis 6
	ProtoResp3 Protocol = "RESP3"
)

// Config represents config of Cache
type Config struct {
	// redis server address
	// in case of RESP2ClusterProxy protocol, it is the address of the proxy
	ServerAddr string

	// Redis password
	Password string

	// size of the  in memory cache
	// Default is 10K
	CacheSize int

	// Protocol of redis being used.
	//
	// The default is ProtoResp2
	Protocol Protocol

	// Logger to be used, the default logger will print nothing
	Logger logger.Logger

	// ClusterNodes is a list of cluster nodes
	// only being used by ProtoResp2ClusterProxy protocol.
	ClusterNodes []string

	DataPool resp2.DataPool
}

// Rimcu is a redis client which implements client side caching.
// It is safe for concurrent use by multiples goroutine
type Rimcu struct {
	serverAddr   string
	logger       logger.Logger
	protocol     Protocol
	clusterNodes []string
	password     string
	dataPool     resp2.DataPool
}

// New creates a new Rimcu redis client
func New(cfg Config) *Rimcu {
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}
	return &Rimcu{
		serverAddr:   cfg.ServerAddr,
		logger:       cfg.Logger,
		protocol:     cfg.Protocol,
		clusterNodes: cfg.ClusterNodes,
		password:     cfg.Password,
		dataPool:     cfg.DataPool,
	}
}

// NewStringsCache creates a new strings cache and do the required initialization
func (r *Rimcu) NewStringsCache(cfg StringsCacheConfig) (*StringsCache, error) {
	cfg.logger = r.logger
	cfg.serverAddr = r.serverAddr
	cfg.protocol = r.protocol
	cfg.clusterNodes = r.clusterNodes
	cfg.password = r.password
	cfg.dataPool = r.dataPool
	return newStringsCache(cfg)
}

const (
	defaultCacheSize   = 100000
	defaultCacheTTLSec = 60 * 20
)
