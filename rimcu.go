package rimcu

import (
	"errors"
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	cmdSet = "SET"
	cmdGet = "GET"
	cmdDel = "DEL"
)
