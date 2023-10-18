package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"runtime/pprof"

	"github.com/keboola/processor-split-table/internal/pkg/config"
	"github.com/keboola/processor-split-table/internal/pkg/finder"
	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/processor"
)

func main() {
	// Handle panic with correct exit code
	defer handlePanic()

	// Remove timestamp prefix from logs
	log.SetFlags(0)

	// Cpu profiling can be enabled by flag
	if startCPUProfileIfFlagSet() {
		defer pprof.StopCPUProfile()
	}

	logger := log.New(os.Stdout, "", 0)
	conf := config.LoadConfig(kbc.GetDataDir() + "/config.json")
	inputDir := kbc.GetInputDir()
	outputDir := kbc.GetOutputDir()
	files := finder.FindFilesRecursive(inputDir)
	processor.NewProcessor(logger, conf, inputDir, outputDir, files).Run()
}

func handlePanic() {
	if err := recover(); err != nil {
		var msg string
		var exitCode int

		switch v := err.(type) {
		case *kbc.Error:
			// Load exit code from error if possible
			msg = v.Error()
			exitCode = v.ExitCode()
		default:
			// ApplicationError by default
			msg = fmt.Sprintln(err)
			exitCode = 2
		}

		// Print error
		log.Println(msg)

		// Log stack trace for Application Error
		if exitCode > 1 {
			log.Println("Trace: \n" + string(debug.Stack()))
		}

		os.Exit(exitCode)
	}
}

func startCPUProfileIfFlagSet() bool {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			kbc.PanicApplicationErrorf("%s", err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			kbc.PanicApplicationErrorf("%s", err)
		}
		return true
	}

	return false
}
