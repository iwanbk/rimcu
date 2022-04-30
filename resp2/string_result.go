package resp2

import (
	"github.com/iwanbk/rimcu/internal/redigo/redis"
)

// StringResult represents result of the strings cache read operation
type StringResult struct {
	val interface{}
}

func newStringResult(val interface{}) *StringResult {
	return &StringResult{
		val: val,
	}
}

// Bool returns boolean representation of the resul
func (sr *StringResult) Bool() (bool, error) {
	return redis.Bool(sr.val, nil)
}

// String returns string representation of the result
func (sr *StringResult) String() (string, error) {
	return redis.String(sr.val, nil)
}
