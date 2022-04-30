package logger

import (
	"log"
)

// Logger defines interface that must be implemented by the user
// who want to log the internal rimcu lib
type Logger interface {
	Debugf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

// NewDefaultLogger creates logger which doing nothing
func NewDefault() Logger {
	return &defaultLogger{}
}

type defaultLogger struct {
}

func (d *defaultLogger) Debugf(format string, v ...interface{}) {
}

func (d *defaultLogger) Errorf(format string, v ...interface{}) {
	log.Printf(format, v...)
}
