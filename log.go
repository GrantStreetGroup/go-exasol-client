package exasol

import (
	"log"
	"os"
)

// By default we'll only print out warnings, errors and fatals to stderr.
// If you want anything else you'll need to pass in a custom logger to the
// connection and it needs to conform to the following interface:

type Logger interface {
	Debug(...interface{})
	Debugf(string, ...interface{})

	Info(...interface{})
	Infof(string, ...interface{})

	Warning(...interface{})
	Warningf(string, ...interface{})

	Error(...interface{})
	Errorf(string, ...interface{})
}

type defLogger struct {
	logger *log.Logger
}

func newDefaultLogger() *defLogger {
	return &defLogger{log.New(os.Stderr, "[exasol]", log.Lshortfile)}
}

func (l *defLogger) Debug(args ...interface{})              {}
func (l *defLogger) Debugf(str string, args ...interface{}) {}

func (l *defLogger) Info(args ...interface{})              {}
func (l *defLogger) Infof(str string, args ...interface{}) {}

func (l *defLogger) Warning(args ...interface{})              { l.logger.Print(args...) }
func (l *defLogger) Warningf(str string, args ...interface{}) { l.logger.Printf(str, args...) }

func (l *defLogger) Error(args ...interface{})              { l.logger.Print(args...) }
func (l *defLogger) Errorf(str string, args ...interface{}) { l.logger.Printf(str, args...) }
