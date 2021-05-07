// Package kbc provides components Common Interface implementation
// https://developers.keboola.com/extend/common-interface/
package kbc

import (
	"fmt"
	"os"
	"strings"
)

type Error struct {
	message  string
	exitCode int
}

func (e *Error) Error() string {
	return e.message
}

// ExitCode is processed in main.go
func (e *Error) ExitCode() int {
	return e.exitCode
}

// UserError logs message and stops program execution with exit code 1
func UserError(format string, a ...interface{}) *Error {
	format = strings.TrimSpace(format)
	return &Error{
		fmt.Sprintf(format, a...),
		1,
	}
}

// ApplicationError logs message and stops program execution with exit code 2
func ApplicationError(format string, a ...interface{}) *Error {
	format = strings.TrimSpace(format)
	return &Error{
		fmt.Sprintf(format, a...),
		2,
	}
}

func PanicApplicationError(format string, a ...interface{}) {
	panic(ApplicationError(format, a...))
}

func PanicUserError(format string, a ...interface{}) {
	panic(UserError(format, a...))
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
