package resp2

import (
	"github.com/iwanbk/rimcu/internal/redigo/redis"
)

// StringResult represents result of the strings cache read operation
type StringResult struct {
	val interface{}
	err error
}

func newStringResult(val interface{}, err error) *StringResult {
	return &StringResult{
		val: val,
		err: err,
	}
}

// Err returns error of the command result, if any
func (sr *StringResult) Err() error {
	return sr.err
}

// String returns string representation of the result
func (sr *StringResult) String() (string, error) {
	if sr.err != nil {
		return "", sr.err
	}
	return redis.String(sr.val, nil)
}
