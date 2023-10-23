package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	en_translations "github.com/go-playground/validator/v10/translations/en"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/processor-split-table/internal/pkg/slicer"
	slicerConfig "github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

const (
	ENVPrefix = "SLICER"
	usageText = `Usage of "slicer".

  Modes via --mode:
      bytes
        New slice is created when the --bytes-per-slice limit is reached.
        Bytes size is measured before compression, if any.

      rows
        New slice is created when the --rows-per-slice limit is reached.

      slices
        The table is split into a fixed --number-of-slices.
        Each slice except the last must have at least --min-bytes-per-slice, it takes precedence.


  Input and output table:
    --table-name
        Table name for logging purposes.
    --table-input-path
        Path to the input table, either a file or a directory with slices.
    --table-input-manifest-path
        Path to the manifest of the input table.
        It is used to get "delimiter" and "enclosure" fields, if any.
        It can be omitted only if the table does not have a manifest.
    --table-output-path
        Directory where the slices of the output table will be written.
        If it does not exist, it will be created, but the parent directory must exist.
    --table-output-manifest-path
        Path where the output manifest will be written.
        The parent directory must exist.
        The output manifest is a copy of the input manifest.
        The "columns" field is set from the CSV header, if it is missing.


  Environment variables:
    Each flag can be specified via an env variable with the "SLICER_" prefix.
    For example --bytes-per-slice flag can be specified via SLICER_BYTES_PER_SLICE env.


  All flags:
`
)

type Config struct {
	slicer.Table   `json:"table"  mapstructure:",squash"`
	DumpConfig     bool   `json:"dumpConfig" mapstructure:"dump-config"`
	CPUProfileFile string `json:"cpuProfile" mapstructure:"cpuprofile"`
}

func DefaultConfig() Config {
	cfg := Config{}
	cfg.Config = slicerConfig.DefaultConfig()
	return cfg
}

func ParseConfig(args []string) (Config, error) {
	cfg := DefaultConfig()

	// Parse flags
	f := flags()
	if err := f.Parse(args); err != nil {
		return cfg, fmt.Errorf("cannot parse flags: %w", err)
	}

	// Bind flags to the config structure
	binder := viper.New()
	binder.AutomaticEnv()
	binder.SetEnvPrefix(ENVPrefix)
	binder.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	if err := binder.BindPFlags(f); err != nil {
		return cfg, fmt.Errorf("cannot bind flags: %w", err)
	}
	if err := binder.Unmarshal(&cfg, viper.DecodeHook(mapstructure.TextUnmarshallerHookFunc())); err != nil {
		return cfg, fmt.Errorf("cannot unmarshal flags: %w", err)
	}

	// Create validator
	v := validator.New()

	// Register fields naming function
	v.RegisterTagNameFunc(func(f reflect.StructField) string {
		if name := strings.SplitN(f.Tag.Get("mapstructure"), ",", 2)[0]; name != "" && name != "-" {
			return name
		}
		if name := strings.SplitN(f.Tag.Get("json"), ",", 2)[0]; name != "" && name != "-" {
			return name
		}
		return ""
	})

	// Setup translator
	lang := en.New()
	trans, _ := ut.New(lang, lang).GetTranslator("en")
	if err := en_translations.RegisterDefaultTranslations(v, trans); err != nil {
		return cfg, err
	}
	if err := trans.Add("required", `{0} is a required flag`, true); err != nil {
		return cfg, err
	}

	// Validate config
	if err := v.Struct(cfg); err != nil {
		var valErrs validator.ValidationErrors
		if errors.As(err, &valErrs) {
			// Generate better error messages via translator
			var b strings.Builder
			for _, item := range valErrs {
				b.WriteString("\n")
				b.WriteString("- ")
				b.WriteString(item.Translate(trans))
			}
			err = errors.New(b.String())
		}
		return cfg, fmt.Errorf(`configuration is not valid:%w`, err)
	}

	return cfg, nil
}

func Usage() string {
	var b strings.Builder
	b.WriteString(usageText)
	b.WriteString(flags().FlagUsages())
	return b.String()
}

func flags() *pflag.FlagSet {
	cfg := DefaultConfig()
	modes := fmt.Sprintf(
		`%s, %s, or %s`,
		slicerConfig.ModeBytes.String(),
		slicerConfig.ModeRows.String(),
		slicerConfig.ModeSlices.String(),
	)

	f := pflag.NewFlagSet("slicer", pflag.ContinueOnError)
	f.Bool("dump-config", cfg.DumpConfig, "Print all parameters to the STDOUT.")
	f.String("cpuprofile", cfg.CPUProfileFile, "Write the CPU profile to the specified file.")

	f.String("table-name", cfg.Name, "Table name for logging purposes.")
	f.String("table-input-path", cfg.InPath, "Path to the input table, either a file or a directory with slices.")
	f.String("table-input-manifest-path", cfg.InManifestPath, "Path to the manifest describing the input table, if any.")
	f.String("table-output-path", cfg.OutPath, "Directory where the slices of the output table will be written.")
	f.String("table-output-manifest-path", cfg.OutManifestPath, "Path where the output manifest will be written.")

	f.String("mode", cfg.Mode.String(), modes)
	f.String("bytes-per-slice", cfg.BytesPerSlice.String(), `Maximum size of a slice, for "bytes"" mode.`)
	f.Uint64("rows-per-slice", cfg.RowsPerSlice, `Maximum number of rows per slice, for "rows" mode.`)
	f.Uint32("number-of-slices", cfg.NumberOfSlices, `Number of slices, for "slices" mode.`)
	f.String("min-bytes-per-slice", cfg.MinBytesPerSlice.String(), `Minimum size of a slice, for "slices" mode.`)

	f.Bool("gzip", cfg.Gzip, "Enable gzip compression for slices.")
	f.Int("gzip-level", cfg.GzipLevel, "GZIP compression level, range: 1 best speed - 9 best compression.")
	f.Uint32("gzip-concurrency", cfg.GzipConcurrency, "Number of parallel processed gzip blocks, 0 means the number of CPU threads.")
	f.String("gzip-block-size", cfg.GzipBlockSize.String(), "Size of the one gzip block; allocated memory = concurrency * block size.")
	f.String("buffer-size", cfg.BufferSize.String(), "Output buffer size when gzip compression is disabled.")

	return f
}
