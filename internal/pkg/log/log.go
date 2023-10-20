package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger interface {
	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
	Debugf(template string, args ...any)
	Infof(template string, args ...any)
	Warnf(template string, args ...any)
	Errorf(template string, args ...any)
}

// NewLogger creates a logger that logs messages by default to STDOUT and Warn/Error levels to STDERR.
func NewLogger() Logger {
	toInfoLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l == zapcore.DebugLevel || l == zapcore.InfoLevel
	})
	fromWarnLevel := zapcore.WarnLevel

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "msg"})
	return zap.New(zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), toInfoLevel),
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stderr), fromWarnLevel),
	)).Sugar()
}
