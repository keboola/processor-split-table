package config

import (
	"github.com/stretchr/testify/assert"
	"keboola.processor-split-table/src/utils"
	"os"
	"testing"
)

type testData struct {
	comment  string
	input    string
	error    string
	expected *Config
}

func TestConfig(t *testing.T) {
	// Create temp dir
	tempDir := t.TempDir()

	for _, testData := range getTestData() {
		// Write content to file
		configPath := tempDir + "/config.json"
		f := utils.OpenFile(configPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		utils.WriteStringToFile(f, testData.input, configPath)
		utils.CloseFile(f, configPath)

		// Test
		if testData.expected != nil {
			conf := LoadConfig(configPath)
			assert.Equal(t, testData.expected, conf, testData.comment)
		}

		if testData.error != "" {
			assert.PanicsWithError(t, testData.error, func() {
				LoadConfig(configPath)
			}, testData.comment)
		}
	}
}

func getTestData() []testData {
	return []testData{
		{
			comment:  "invalid data type",
			input:    "{\"parameters\": \"abc\"}",
			error:    "Invalid configuration: key \"parameters\" has invalid type \"string\".",
			expected: nil,
		},
		{
			comment:  "invalid mode",
			input:    "{\"parameters\": {\"mode\": \"abc\"}}",
			error:    "Invalid configuration: unexpected value \"abc\" for \"mode\". Use \"rows\", \"bytes\" or \"slices\".",
			expected: nil,
		},
		{
			comment:  "min value bytesPerSlice",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"bytesPerSlice\": 0}}",
			error:    "Invalid configuration: key=\"parameters.bytesPerSlice\", value=\"0\" failed on the \"min\" validation.",
			expected: nil,
		},
		{
			comment:  "min value rowsPerSlice",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"rowsPerSlice\": 0}}",
			error:    "Invalid configuration: key=\"parameters.rowsPerSlice\", value=\"0\" failed on the \"min\" validation.",
			expected: nil,
		},
		{
			comment:  "min value gzipLevel",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"gzipLevel\": 0}}",
			error:    "Invalid configuration: key=\"parameters.gzipLevel\", value=\"0\" failed on the \"min\" validation.",
			expected: nil,
		},
		{
			comment:  "max value gzipLevel",
			input:    "{\"parameters\": {\"mode\": \"bytes\", \"gzipLevel\": 10}}",
			error:    "Invalid configuration: key=\"parameters.gzipLevel\", value=\"10\" failed on the \"max\" validation.",
			expected: nil,
		},
		{
			comment: "default values 1",
			input:   "{}",
			error:   "",
			expected: &Config{
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice:    524_288_000,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4194304,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "default values 2",
			input:   "{\"parameters\": {}}",
			error:   "",
			expected: &Config{
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice:    524_288_000,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4194304,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
		{
			comment: "gzip enabled",
			input:   "{\"parameters\": {\"gzip\": true, \"gzipLevel\": 5}}",
			error:   "",
			expected: &Config{
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice:    524_288_000,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4194304,
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
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice:    524_288_000,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4194304,
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
				Parameters: Parameters{Mode: ModeRows,
					BytesPerSlice:    524_288_000,
					RowsPerSlice:     123,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4194304,
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
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice:    123,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 4194304,
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
				Parameters: Parameters{Mode: ModeSlices,
					BytesPerSlice:    524_288_000,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   123,
					MinBytesPerSlice: 4194304,
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
				Parameters: Parameters{Mode: ModeSlices,
					BytesPerSlice:    524_288_000,
					RowsPerSlice:     1_000_000,
					NumberOfSlices:   60,
					MinBytesPerSlice: 123,
					Gzip:             true,
					GzipLevel:        2,
				},
			},
		},
	}
}
