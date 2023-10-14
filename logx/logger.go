package logx

import (
	"log/slog"
	"os"
	"sync"
	"time"

	"golang.org/x/exp/constraints"
)

type Level int

const (
	Linfo Level = iota
	Ldebug
	Ltrace
)

func (l Level) intoSlog() slog.Level {
	switch l {
	case Linfo:
		return slog.LevelInfo
	case Ldebug:
		return slog.LevelDebug
	case Ltrace:
		return levelTrace
	default:
		return slog.LevelInfo
	}
}

type Logger interface {
	Trace(msg string, v ...Attr)
	Debug(msg string, v ...Attr)
	Info(msg string, v ...Attr)
	Warn(msg string, v ...Attr)
	Error(msg string, v ...Attr)
	SetLevel(level Level)
}

var (
	setupOnce sync.Once
	instance  Logger
)

func setup() { setupOnce.Do(setupInstance) }

const (
	levelTrace = slog.Level(-8)
)

func replaceAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey {
		level := a.Value.Any().(slog.Level)
		if level == levelTrace {
			a.Value = slog.StringValue("TRACE")
		}
	}
	return a
}

func setupInstance() {
	levelVar := new(slog.LevelVar)
	instance = &logger{
		Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level:       levelVar,
			ReplaceAttr: replaceAttr,
		})),
		level: levelVar,
	}
}

type logger struct {
	*slog.Logger
	level *slog.LevelVar
}

func (l *logger) Trace(msg string, v ...Attr) { l.logAttrs(levelTrace, msg, v...) }
func (l *logger) Debug(msg string, v ...Attr) { l.logAttrs(slog.LevelDebug, msg, v...) }
func (l *logger) Info(msg string, v ...Attr)  { l.logAttrs(slog.LevelInfo, msg, v...) }
func (l *logger) Warn(msg string, v ...Attr)  { l.logAttrs(slog.LevelWarn, msg, v...) }
func (l *logger) Error(msg string, v ...Attr) { l.logAttrs(slog.LevelError, msg, v...) }

func (l *logger) logAttrs(level slog.Level, msg string, v ...Attr) {
	attrs := make([]slog.Attr, len(v))
	for i, attr := range v {
		attrs[i] = slog.Attr(attr)
	}
	l.LogAttrs(nil, level, msg, attrs...)
}

func (l *logger) SetLevel(level Level) { l.level.Set(level.intoSlog()) }

func getInstance() Logger {
	setup()
	return instance
}

// G returns the global logger.
func G() Logger { return getInstance() }

type Attr slog.Attr

func S(k, v string) Attr                                   { return Attr(slog.String(k, v)) }
func SS(k string, v []string) Attr                         { return Attr(slog.Any(k, v)) }
func Any(k string, v any) Attr                             { return Attr(slog.Any(k, v)) }
func I[T constraints.Integer](k string, v T) Attr          { return Attr(slog.Int(k, int(v))) }
func II[U ~[]T, T constraints.Integer](k string, v U) Attr { return Attr(slog.Any(k, v)) }
func Err(err error) Attr                                   { return Attr(slog.Any("error", err)) }
func D(k string, v time.Duration) Attr                     { return Attr(slog.Duration(k, v)) }
func Group(k string, args ...Attr) Attr                    { return Attr(slog.Any(k, args)) }
