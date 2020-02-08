package resp2

import (
	"log"
)

type Logger interface {
	Debugf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

type defaultLogger struct {
}

func (d *defaultLogger) Debugf(format string, v ...interface{}) {
}

func (d *defaultLogger) Errorf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

type debugLogger struct {
}

func (d *debugLogger) Debugf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (d *debugLogger) Errorf(format string, v ...interface{}) {
	log.Printf(format, v...)
}
