package config

import (
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	slicerConfig "github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

type testData struct {
	comment  string
	input    string
	error    string
	expected *Config
}

func TestConfig(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	for _, testData := range getTestData() {
		// Write content to file
		configPath := tempDir + "/config.json"
		assert.NoError(t, os.WriteFile(configPath, []byte(testData.input), 0o0644))

		// Test
		conf, err := LoadConfig(configPath)
		if testData.expected != nil {
			require.NoError(t, err)
			assert.Equal(t, testData.expected, conf, testData.comment)
		} else if testData.error != "" {
			if assert.Error(t, err) {
				assert.Equal(t, testData.error, err.Error())
			}
		}
	}
}

func getTestData() []testData {
	return []testData{
		{
			comment:  "invalid data type",
			input:    "{\"parameters\": \"abc\"}",
			error:    `invalid configuration: key "parameters" has invalid type "string"`,
			expected: nil,
		},
		{
			comment:  "invalid mode",
			input:    "{\"parameters\": {\"mode\": \"abc\"}}",
			error:    `invalid configuration: unexpected value "abc" for "mode", use "rows", "bytes" or "slices"`,
			expected: nil,
		},
		{
			comment:  "min value bytesPerSlice",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"bytesPerSlice\": 0}}",
			error:    `invalid configuration: key="parameters.bytesPerSlice", value="0B" failed on the "min" validation`,
			expected: nil,
		},
		{
			comment:  "min value rowsPerSlice",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"rowsPerSlice\": 0}}",
			error:    `invalid configuration: key="parameters.rowsPerSlice", value="0" failed on the "min" validation`,
			expected: nil,
		},
		{
			comment:  "min value gzipLevel",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"gzipLevel\": 0}}",
			error:    `invalid configuration: key="parameters.gzipLevel", value="0" failed on the "min" validation`,
			expected: nil,
		},
		{
			comment:  "max value gzipLevel",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"gzipLevel\": 10}}",
			error:    `invalid configuration: key="parameters.gzipLevel", value="10" failed on the "max" validation`,
			expected: nil,
		},
		{
			comment:  "default values 1",
			input:    "{}",
			error:    "",
			expected: &Config{Parameters: slicerConfig.DefaultConfig()},
		},
		{
			comment:  "default values 2",
			input:    "{\"parameters\": {}}",
			error:    "",
			expected: &Config{Parameters: slicerConfig.DefaultConfig()},
		},
		{
			comment: "gzip enabled",
			input:   "{\"parameters\": {\"gzip\": true, \"gzipLevel\": 5}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeBytes,
					BytesPerSlice:    500 * datasize.MB,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4 * datasize.MB,
					Gzip:             true,
					GzipLevel:        5,
				},
			},
		},
		{
			comment: "gzip disabled",
			input:   "{\"parameters\": {\"gzip\": false}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeBytes,
					BytesPerSlice:    500 * datasize.MB,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4 * datasize.MB,
					Gzip:             false,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "mode-rows",
			input:   "{\"parameters\": {\"mode\": \"rows\", \"rowsPerSlice\": 123}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeRows,
					BytesPerSlice:    500 * datasize.MB,
					RowsPerSlice:     123,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4 * datasize.MB,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "mode-bytes",
			input:   "{\"parameters\": {\"mode\": \"bytes\", \"bytesPerSlice\": 123}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeBytes,
					BytesPerSlice:    123,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4 * datasize.MB,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "mode-bytes-string",
			input:   "{\"parameters\": {\"mode\": \"bytes\", \"bytesPerSlice\": \"1KB\"}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeBytes,
					BytesPerSlice:    1 * datasize.KB,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4 * datasize.MB,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "mode-slices",
			input:   "{\"parameters\": {\"mode\": \"slices\", \"numberOfSlices\": 123}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeSlices,
					BytesPerSlice:    500 * datasize.MB,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   123,
					MinBytesPerSlice: 4 * datasize.MB,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "mode-slices-min-size",
			input:   "{\"parameters\": {\"mode\": \"slices\", \"minBytesPerSlice\": 123}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeSlices,
					BytesPerSlice:    500 * datasize.MB,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 123,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "mode-slices-min-size-string",
			input:   "{\"parameters\": {\"mode\": \"slices\", \"minBytesPerSlice\": \"2KB\"}}",
			error:   "",
			expected: &Config{
				Parameters: slicerConfig.Config{
					Mode:             slicerConfig.ModeSlices,
					BytesPerSlice:    500 * datasize.MB,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 2 * datasize.KB,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
	}
}
