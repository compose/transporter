// Much of this comes from https://github.com/prometheus/common/blob/master/log/log.go

package log

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
)

type levelFlag string

// String implements flag.Value.
func (f levelFlag) String() string {
	return fmt.Sprintf("%q", string(f))
}

// Set implements flag.Value.
func (f levelFlag) Set(level string) error {
	l, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	origLogger.Level = l
	return nil
}

func init() {
	AddFlags(flag.CommandLine)
}

// AddFlags adds the flags used by this package to the given FlagSet. That's
// useful if working with a custom FlagSet. The init function of this package
// adds the flags to flag.CommandLine anyway. Thus, it's usually enough to call
// flag.Parse() to make the logging flags take effect.
func AddFlags(fs *flag.FlagSet) {
	fs.Var(
		levelFlag(origLogger.Level.String()),
		"log.level",
		"Only log messages with the given severity or above. Valid levels: [debug, info, error]",
	)
}

// Logger is the interface for loggers used in transporter components
type Logger interface {
	Debugln(...interface{})
	Debugf(string, ...interface{})

	Infoln(...interface{})
	Infof(string, ...interface{})

	Errorln(...interface{})
	Errorf(string, ...interface{})

	Printf(string, ...interface{})

	With(key string, value interface{}) Logger
}

type logger struct {
	entry *logrus.Entry
}

func (l logger) With(key string, value interface{}) Logger {
	return logger{l.entry.WithField(key, value)}
}

// Debug logs a message at level Debug on the standard logger.
func (l logger) Debugln(args ...interface{}) {
	l.entry.Debugln(args...)
}

// Debugf logs a message at level Debug on the standard logger.
func (l logger) Debugf(format string, args ...interface{}) {
	l.entry.Debugf(format, args...)
}

// Info logs a message at level Info on the standard logger.
func (l logger) Infoln(args ...interface{}) {
	l.entry.Infoln(args...)
}

// Infof logs a message at level Info on the standard logger.
func (l logger) Infof(format string, args ...interface{}) {
	l.entry.Infof(format, args...)
}

// Error logs a message at level Error on the standard logger.
func (l logger) Errorln(args ...interface{}) {
	l.entry.Errorln(args...)
}

// Errorf logs a message at level Error on the standard logger.
func (l logger) Errorf(format string, args ...interface{}) {
	l.entry.Errorf(format, args...)
}

func (l logger) Printf(format string, args ...interface{}) {
	l.entry.Infof(format, args...)
}

var origLogger = logrus.New()
var baseLogger = logger{entry: logrus.NewEntry(origLogger)}

// Base returns the default Logger logging to
func Base() Logger {
	return baseLogger
}

// NewLogger returns a new Logger logging to out.
func NewLogger(w io.Writer) Logger {
	l := logrus.New()
	l.Out = w
	return logger{entry: logrus.NewEntry(l)}
}

// NewNopLogger returns a logger that discards all log messages.
func NewNopLogger() Logger {
	l := logrus.New()
	l.Out = ioutil.Discard
	return logger{entry: logrus.NewEntry(l)}
}

// With adds a field to the logger.
func With(key string, value interface{}) Logger {
	return baseLogger.With(key, value)
}

// Debugln logs a message at level Debug on the standard logger.
func Debugln(args ...interface{}) {
	baseLogger.Debugln(args...)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, args ...interface{}) {
	baseLogger.Debugf(format, args...)
}

// Infoln logs a message at level Info on the standard logger.
func Infoln(args ...interface{}) {
	baseLogger.Infoln(args...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(format string, args ...interface{}) {
	baseLogger.Infof(format, args...)
}

// Errorln logs a message at level Error on the standard logger.
func Errorln(args ...interface{}) {
	baseLogger.Errorln(args...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(format string, args ...interface{}) {
	baseLogger.Errorf(format, args...)
}
