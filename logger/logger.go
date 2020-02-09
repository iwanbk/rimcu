package logger

import (
	"log"
)

type Logger interface {
	Debugf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

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
