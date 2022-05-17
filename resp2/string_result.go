package resp2

import (
	"github.com/iwanbk/rimcu/internal/redigo/redis"
)

// StringResult represents result of the strings cache read operation
type StringResult struct {
	val            interface{}
	fromLocalCache bool
}

func newStringResult(val interface{}, fromLocalCache bool) *StringResult {
	return &StringResult{
		val:            val,
		fromLocalCache: fromLocalCache,
	}
}

// Bool returns boolean representation of the result
func (sr *StringResult) Bool() (bool, error) {
	return redis.Bool(sr.val, nil)
}

// String returns string representation of the result
func (sr *StringResult) String() (string, error) {
	return redis.String(sr.val, nil)
}

// FromLocalCache returns true if this result was coming from
// the local inmemory cache
func (sr *StringResult) FromLocalCache() bool {
	return sr.fromLocalCache
}
