package main

import (
	"flag"
	"fmt"
	"keboola.processor-split-table/src/config"
	"keboola.processor-split-table/src/finder"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/processor"
	"log"
	"os"
	"runtime/debug"
	"runtime/pprof"
)

func main() {
	// Handle panic with correct exit code
	defer handlePanic()

	// Remove timestamp prefix from logs
	log.SetFlags(0)

	// Cpu profiling can be enabled by flag
	if startCpuProfileIfFlagSet() {
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

func startCpuProfileIfFlagSet() bool {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			kbc.PanicApplicationError("%s", err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			kbc.PanicApplicationError("%s", err)
		}
		return true
	}

	return false
}
