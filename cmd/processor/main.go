package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"runtime/pprof"

	"github.com/keboola/processor-split-table/internal/pkg/config"
	"github.com/keboola/processor-split-table/internal/pkg/finder"
	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/log"
	"github.com/keboola/processor-split-table/internal/pkg/processor"
)

func main() {
	logger := log.NewLogger()

	// Handle panic with correct exit code
	defer func() {
		if err := recover(); err != nil {
			exitWithError(logger, err)
		}
	}()

	// Cpu profiling can be enabled by flag
	if started, err := startCPUProfileIfFlagSet(); err != nil {
		exitWithError(logger, err)
	} else if started {
		defer pprof.StopCPUProfile()
	}

	if err := run(logger); err != nil {
		exitWithError(logger, err)
	}
}

func run(logger log.Logger) error {
	inputDir := kbc.GetInputDir()
	outputDir := kbc.GetOutputDir()

	// Load config
	conf, err := config.LoadConfig(kbc.GetDataDir() + "/config.json")
	if err != nil {
		return err
	}

	// Find files
	files, err := finder.FindFilesRecursive(inputDir)
	if err != nil {
		return err
	}

	// Process files
	if err := processor.NewProcessor(logger, conf, inputDir, outputDir, files).Run(); err != nil {
		return err
	}

	return nil
}

func exitWithError(logger log.Logger, err any) {
	// Get message
	var msg string
	if e, ok := err.(error); ok {
		msg = e.Error()
	} else {
		msg = fmt.Sprintf("%v", msg)
	}

	// Get exit code
	exitCode := 2 // application error by default
	if e, ok := err.(kbc.Error); ok {
		exitCode = e.ExitCode()
	}

	// Print message
	logger.Error("Error: ", msg)

	// Log stack trace for Application Error
	if exitCode > 1 {
		logger.Error("Trace: \n" + string(debug.Stack()))
	}

	os.Exit(exitCode)
}

func startCPUProfileIfFlagSet() (bool, error) {
	ptr := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()
	if *ptr != "" {
		f, err := os.Create(*ptr)
		if err != nil {
			return false, err
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}
