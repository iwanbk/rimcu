package rimcu

import (
	"errors"
)

var (
	// ErrNotFound returned when the value of the key is not exists
	ErrNotFound = errors.New("not found")

	// ErrInvalidArgs returned when the user pass invalid arguments to the func
	ErrInvalidArgs = errors.New("invalid arguments")
)

const (
	cmdSet    = "SET"
	cmdGet    = "GET"
	cmdDel    = "DEL"
	cmdAppend = "APPEND"
	cmdMSet   = "MSET"
	cmdMGet   = "MGET"
)
