package cli

import (
	"encoding/json"
	"os"
	"runtime/debug"
	"runtime/pprof"

	"github.com/spf13/pflag"

	"github.com/keboola/processor-split-table/internal/pkg/cli/config"
	"github.com/keboola/processor-split-table/internal/pkg/log"
	"github.com/keboola/processor-split-table/internal/pkg/slicer"
)

func Run(logger log.Logger) error {
	// Parse flags and ENVs
	cfg, err := config.Parse(os.Args)
	if cfg.Help {
		printUsage()
		return pflag.ErrHelp
	} else if err != nil {
		return err
	}

	// Set soft memory limit (GOMEMLIMIT)
	debug.SetMemoryLimit(int64(cfg.MemoryLimit.Bytes()))

	// Dump configuration to STDOUT
	if cfg.DumpConfig {
		out, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return err
		}
		logger.Info("Configuration: ", string(out))
	}

	// Cpu profiling can be enabled by flag
	if started, err := startCPUProfile(cfg.CPUProfileFile); err != nil {
		return err
	} else if started {
		defer pprof.StopCPUProfile()
	}

	// In CLI, the manifest must exist, if it is specified by the flag.
	// In processor, the manifest is autodetect, so it may not exist.
	cfg.InManifestMustExists = true

	// Slice table
	return slicer.SliceTable(logger, cfg.Table)
}

func startCPUProfile(path string) (bool, error) {
	if path != "" {
		f, err := os.Create(path)
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

func printUsage() {
	_, _ = os.Stderr.WriteString(config.Usage())
}
