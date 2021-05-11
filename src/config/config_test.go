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
			error:    "Invalid configuration: unexpected value \"abc\" for \"mode\". Use \"rows\" or \"bytes\".",
			expected: nil,
		},
		{
			comment: "default values 1",
			input:   "{}",
			error:   "",
			expected: &Config{
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice: 524_288_000,
					RowsPerSlice:  1_000_000},
			},
		},
		{
			comment: "default values 2",
			input:   "{\"parameters\": {}}",
			error:   "",
			expected: &Config{
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice: 524_288_000,
					RowsPerSlice:  1_000_000,
				},
			},
		},
		{
			comment: "full-rows",
			input:   "{\"parameters\": {\"mode\": \"rows\", \"rowsPerSlice\": 123}}",
			error:   "",
			expected: &Config{
				Parameters: Parameters{Mode: ModeRows,
					BytesPerSlice: 524_288_000,
					RowsPerSlice:  123,
				},
			},
		},
		{
			comment: "full-bytes",
			input:   "{\"parameters\": {\"mode\": \"bytes\", \"bytesPerSlice\": 123}}",
			error:   "",
			expected: &Config{
				Parameters: Parameters{Mode: ModeBytes,
					BytesPerSlice: 123,
					RowsPerSlice:  1_000_000,
				},
			},
		},
	}
}
