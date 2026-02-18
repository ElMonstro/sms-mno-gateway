package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

// Logger is the application logger interface
type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	WithField(key string, value interface{}) Logger
	WithFields(fields map[string]interface{}) Logger
	WithError(err error) Logger
}

// logrusLogger wraps logrus.Entry to implement Logger interface
type logrusLogger struct {
	entry *logrus.Entry
}

// New creates a new logger with the specified level
func New(level string) Logger {
	log := logrus.New()
	log.SetOutput(os.Stdout)
	log.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
	})

	parsedLevel, err := logrus.ParseLevel(level)
	if err != nil {
		parsedLevel = logrus.InfoLevel
	}
	log.SetLevel(parsedLevel)

	return &logrusLogger{
		entry: logrus.NewEntry(log),
	}
}

// NewWithLogrus creates a logger from an existing logrus.Logger
func NewWithLogrus(log *logrus.Logger) Logger {
	return &logrusLogger{
		entry: logrus.NewEntry(log),
	}
}

func (l *logrusLogger) Debug(args ...interface{}) {
	l.entry.Debug(args...)
}

func (l *logrusLogger) Debugf(format string, args ...interface{}) {
	l.entry.Debugf(format, args...)
}

func (l *logrusLogger) Info(args ...interface{}) {
	l.entry.Info(args...)
}

func (l *logrusLogger) Infof(format string, args ...interface{}) {
	l.entry.Infof(format, args...)
}

func (l *logrusLogger) Warn(args ...interface{}) {
	l.entry.Warn(args...)
}

func (l *logrusLogger) Warnf(format string, args ...interface{}) {
	l.entry.Warnf(format, args...)
}

func (l *logrusLogger) Error(args ...interface{}) {
	l.entry.Error(args...)
}

func (l *logrusLogger) Errorf(format string, args ...interface{}) {
	l.entry.Errorf(format, args...)
}

func (l *logrusLogger) Fatal(args ...interface{}) {
	l.entry.Fatal(args...)
}

func (l *logrusLogger) Fatalf(format string, args ...interface{}) {
	l.entry.Fatalf(format, args...)
}

func (l *logrusLogger) WithField(key string, value interface{}) Logger {
	return &logrusLogger{
		entry: l.entry.WithField(key, value),
	}
}

func (l *logrusLogger) WithFields(fields map[string]interface{}) Logger {
	return &logrusLogger{
		entry: l.entry.WithFields(fields),
	}
}

func (l *logrusLogger) WithError(err error) Logger {
	return &logrusLogger{
		entry: l.entry.WithError(err),
	}
}

// Noop logger for testing
type noopLogger struct{}

// NewNoop creates a no-op logger that discards all output
func NewNoop() Logger {
	return &noopLogger{}
}

func (l *noopLogger) Debug(args ...interface{})                       {}
func (l *noopLogger) Debugf(format string, args ...interface{})       {}
func (l *noopLogger) Info(args ...interface{})                        {}
func (l *noopLogger) Infof(format string, args ...interface{})        {}
func (l *noopLogger) Warn(args ...interface{})                        {}
func (l *noopLogger) Warnf(format string, args ...interface{})        {}
func (l *noopLogger) Error(args ...interface{})                       {}
func (l *noopLogger) Errorf(format string, args ...interface{})       {}
func (l *noopLogger) Fatal(args ...interface{})                       {}
func (l *noopLogger) Fatalf(format string, args ...interface{})       {}
func (l *noopLogger) WithField(key string, value interface{}) Logger  { return l }
func (l *noopLogger) WithFields(fields map[string]interface{}) Logger { return l }
func (l *noopLogger) WithError(err error) Logger                      { return l }
