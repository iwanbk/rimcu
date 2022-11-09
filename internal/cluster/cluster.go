package cluster

import (
	"fmt"
	"log"
	"strings"

	"github.com/iwanbk/rimcu/internal/redigo/redis"
)

const (
	roleMaster = "master"
	roleSlave  = "slave"
)

// ClusterInfo stores all things related to the cluster
type ClusterInfo struct {
	Shards map[string]Shard
}

// Masters returns address of all the cluster master
func (ci ClusterInfo) Masters() []string {
	masters := make([]string, 0, len(ci.Shards))
	for _, shard := range ci.Shards {
		masters = append(masters, shard.Master.Addr)
	}
	return masters
}

// Slaves returns address of all the cluster slaves
func (ci ClusterInfo) Slaves() []string {
	slaves := make([]string, 0, len(ci.Shards))
	for _, shard := range ci.Shards {
		slaves = append(slaves, shard.Slaves[0].Addr)
	}
	return slaves
}

// Shard represent a redis cluster shard
type Shard struct {
	Master Node
	Slaves []Node
}

// Node represents a redis cluster node, could be master or slave
type Node struct {
	ID       string
	Addr     string
	Role     string
	MasterID string
}

// Explorer is a cluster explorer which the job is mainly to do cluster discovery
type Explorer struct {
	// cluster seeds
	seeds []string
	pools []*redis.Pool
}

// NewExplorer creates new explorer object
func NewExplorer(seeds []string, password string) *Explorer {
	var (
		pools []*redis.Pool
		opts  []redis.DialOption
	)
	if password != "" {
		opts = append(opts, redis.DialPassword(password))
	}

	for _, seed := range seeds {
		pool := &redis.Pool{

			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", seed, opts...)
			},
		}
		pools = append(pools, pool)
	}

	return &Explorer{
		seeds: seeds,
		pools: pools,
	}
}

// Discover the cluster topology
func (ex *Explorer) Discover() (ClusterInfo, error) {
	pool := ex.pools[0]
	conn := pool.Get()
	defer conn.Close()

	str, err := redis.String(conn.Do("cluster", "nodes"))
	if err != nil {
		return ClusterInfo{}, err
	}

	var (
		nodes  = map[string]Node{}
		shards = map[string]Shard{}
	)

	lines := strings.Split(str, "\n")
	for _, line := range lines {
		if !strings.Contains(line, "connected") {
			continue
		}
		words := strings.Split(line, " ")
		if len(words) < 5 {
			continue
		}
		node := Node{
			ID:   words[0],
			Addr: ex.getAddr(words[1]),
			Role: ex.getRole(words[2]),
		}
		if node.Role == roleSlave {
			node.MasterID = words[3]
		}
		log.Printf("node=\n%+v", node)
		nodes[node.ID] = node
		if node.Role == roleMaster {
			shards[node.ID] = Shard{
				Master: node,
			}
		}
	}

	// assign slaves to it's master
	for _, node := range nodes {
		if node.Role == roleMaster {
			continue
		}
		shard, ok := shards[node.MasterID]
		if !ok {
			return ClusterInfo{}, fmt.Errorf("shard not found for slave with master ID:%v", node.MasterID)
		}
		shard.Slaves = append(shard.Slaves, node)
		shards[node.MasterID] = shard
	}

	return ClusterInfo{
		Shards: shards,
	}, nil
}

func (ex *Explorer) getRole(role string) string {
	words := strings.Split(role, ",")
	if len(words) == 1 {
		return words[0]
	}
	return words[1]
}

func (ex *Explorer) getAddr(line string) string {
	elems := strings.Split(line, "@")
	return elems[0]
}
