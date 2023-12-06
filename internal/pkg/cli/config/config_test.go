package config

import (
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

func TestUsage(t *testing.T) {
	t.Parallel()
	assert.NotEmpty(t, Usage()) // asserted in the "cli/help" E2E test
}

func TestParseConfig_Help(t *testing.T) {
	t.Parallel()

	cfg, err := Parse([]string{"--help"})
	require.Error(t, err)
	assert.True(t, cfg.Help)
}

func TestParseConfig_Empty(t *testing.T) {
	t.Parallel()

	_, err := Parse([]string{})
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

	cfg, err := Parse([]string{
		"--table-name", "my-table",
		"--table-input-path", "in/tables/my.csv",
		"--table-output-path", "out/tables/my.csv",
		"--table-output-manifest-path", "out/tables/my.csv.manifest",
	})
	assert.NoError(t, err)

	expected := Default()
	expected.Name = "my-table"
	expected.InPath = "in/tables/my.csv"
	expected.OutPath = "out/tables/my.csv"
	expected.OutManifestPath = "out/tables/my.csv.manifest"
	assert.Equal(t, expected, cfg)
}

func TestParseConfig_Full(t *testing.T) {
	t.Parallel()

	cfg, err := Parse([]string{
		"--buffer-size", "123KB",
		"--bytes-per-slice", "1MB",
		"--cpuprofile", "cpu.out",
		"--gzip=false",
		"--gzip-block-size", "2MB",
		"--gzip-concurrency", "5",
		"--gzip-level", "4",
		"--memory-limit", "128MB",
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
	expected.AheadSlices = 1
	expected.AheadBlocks = 16
	expected.AheadBlockSize = datasize.MB
	expected.Gzip = false
	expected.GzipBlockSize = 2 * datasize.MB
	expected.GzipConcurrency = 5
	expected.GzipLevel = 4
	expected.MemoryLimit = 128 * datasize.MB
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
