// Package kbc provides components Common Interface implementation
// https://developers.keboola.com/extend/common-interface/
package kbc

import (
	"fmt"
	"os"
	"strings"
)

const (
	NewFilePermissions = 0o600
	GzipFileExtension  = ".gz"
)

type Error interface {
	error
	ExitCode() int
}

// UserError is an expected error that should be displayed to the user.
// It triggers exit code 1.
type UserError struct {
	error
}

// ExitCode is processed in main.go.
func (e UserError) ExitCode() int {
	return 1
}

// UserErrorf logs message and stops program execution with exit code 1.
func UserErrorf(format string, a ...interface{}) error {
	format = strings.TrimSpace(format)
	return &UserError{error: fmt.Errorf(format, a...)}
}

func GetDataDir() string {
	return strings.TrimRight(getEnv("KBC_DATADIR", "/data"), "/")
}

func GetInputDir() string {
	return GetDataDir() + "/in"
}

func GetOutputDir() string {
	return GetDataDir() + "/out"
}

func getEnv(key, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		value = fallback
	}
	return value
}
