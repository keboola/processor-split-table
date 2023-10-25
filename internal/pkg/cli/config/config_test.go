package config

import (
	"errors"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

func TestUsage(t *testing.T) {
	t.Parallel()
	assert.Equal(t, strings.TrimLeft(`
Usage of "slicer".

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
      --buffer-size string                  Output buffer size when gzip compression is disabled. (default "20MB")
      --bytes-per-slice string              Maximum size of a slice, for "bytes"" mode. (default "500MB")
      --cpuprofile string                   Write the CPU profile to the specified file.
      --dump-config                         Print all parameters to the STDOUT.
      --gzip                                Enable gzip compression for slices. (default true)
      --gzip-block-size string              Size of the one gzip block; allocated memory = concurrency * block size. (default "2MB")
      --gzip-concurrency uint32             Number of parallel processed gzip blocks, 0 means the number of CPU threads.
      --gzip-level int                      GZIP compression level, range: 1 best speed - 9 best compression. (default 2)
      --min-bytes-per-slice string          Minimum size of a slice, for "slices" mode. (default "4MB")
      --mode string                         bytes, rows, or slices (default "bytes")
      --number-of-slices uint32             Number of slices, for "slices" mode. (default 60)
      --rows-per-slice uint                 Maximum number of rows per slice, for "rows" mode. (default 1000000)
      --table-input-manifest-path string    Path to the manifest describing the input table, if any.
      --table-input-path string             Path to the input table, either a file or a directory with slices.
      --table-name string                   Table name for logging purposes.
      --table-output-manifest-path string   Path where the output manifest will be written.
      --table-output-path string            Directory where the slices of the output table will be written.
`, "\n"), Usage())
}

func TestParseConfig_Help(t *testing.T) {
	t.Parallel()

	_, err := ParseConfig([]string{"--help"})
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, pflag.ErrHelp))
	}
}

func TestParseConfig_Empty(t *testing.T) {
	t.Parallel()

	_, err := ParseConfig([]string{})
	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
configuration is not valid:
- table-name is a required flag
- table-input-path is a required flag
- table-output-path is a required flag
- table-output-manifest-path is a required flag
`), err.Error())
	}
}

func TestParseConfig_Minimal(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]string{
		"--table-name", "my-table",
		"--table-input-path", "in/tables/my.csv",
		"--table-output-path", "out/tables/my.csv",
		"--table-output-manifest-path", "out/tables/my.csv.manifest",
	})
	assert.NoError(t, err)

	expected := DefaultConfig()
	expected.Name = "my-table"
	expected.InPath = "in/tables/my.csv"
	expected.OutPath = "out/tables/my.csv"
	expected.OutManifestPath = "out/tables/my.csv.manifest"
	assert.Equal(t, expected, cfg)
}

func TestParseConfig_Full(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]string{
		"--buffer-size", "123KB",
		"--bytes-per-slice", "1MB",
		"--cpuprofile", "cpu.out",
		"--gzip=false",
		"--gzip-block-size", "2MB",
		"--gzip-concurrency", "5",
		"--gzip-level", "4",
		"--min-bytes-per-slice", "3MB",
		"--mode", "rows",
		"--number-of-slices", "456",
		"--rows-per-slice", "789",
		"--table-name", "my-table",
		"--table-input-path", "in/tables/my.csv",
		"--table-output-path", "out/tables/my.csv",
		"--table-output-manifest-path", "out/tables/my.csv.manifest",
	})
	assert.NoError(t, err)

	expected := Config{}

	expected.BufferSize = 123 * datasize.KB
	expected.BytesPerSlice = 1 * datasize.MB
	expected.CPUProfileFile = "cpu.out"
	expected.Gzip = false
	expected.GzipBlockSize = 2 * datasize.MB
	expected.GzipConcurrency = 5
	expected.GzipLevel = 4
	expected.MinBytesPerSlice = 3 * datasize.MB
	expected.Mode = config.ModeRows
	expected.NumberOfSlices = 456
	expected.RowsPerSlice = 789

	expected.Name = "my-table"
	expected.InPath = "in/tables/my.csv"
	expected.OutPath = "out/tables/my.csv"
	expected.OutManifestPath = "out/tables/my.csv.manifest"

	assert.Equal(t, expected, cfg)
}
