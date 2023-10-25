package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"github.com/keboola/processor-split-table/internal/pkg/cli"
	"github.com/keboola/processor-split-table/internal/pkg/log"
)

func main() {
	logger := log.NewLogger()

	// Handle panic with correct exit code
	defer func() {
		if err := recover(); err != nil {
			exitWithError(logger, err)
		}
	}()

	if err := cli.Run(logger); err != nil {
		exitWithError(logger, err)
	}
}

func exitWithError(logger log.Logger, err any) {
	// Skip help message error
	if e, ok := err.(error); ok {
		if errors.Is(e, pflag.ErrHelp) {
			os.Exit(1)
		}
	}

	// Get message
	var msg string
	if e, ok := err.(error); ok {
		msg = e.Error()
	} else {
		msg = fmt.Sprintf("%v", msg)
	}

	// Print message
	logger.Error("Error: ", msg)
	os.Exit(1)
}
