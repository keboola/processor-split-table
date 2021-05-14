package manifest

import (
	"github.com/stretchr/testify/assert"
	"keboola.processor-split-table/src/utils"
	"os"
	"testing"
)

type testData struct {
	input   string
	columns []string
	output  map[string]interface{}
}

func TestGetSetColumns(t *testing.T) {
	for _, testData := range getTestData() {
		// Create temp dir
		tempDir := t.TempDir()

		// Create test manifest.json
		manifestPath := tempDir + "/manifest.json"
		if testData.input != "" {
			f := utils.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE)
			utils.WriteStringToFile(f, testData.input, manifestPath)
			utils.CloseFile(f, manifestPath)
		}

		// Check expected columns
		manifest := LoadManifest(manifestPath)
		assert.Equal(t, testData.columns, manifest.GetColumns())

		// Set different columns
		newColumns := []string{"1", "2", "3"}
		manifest.SetColumns(newColumns)
		assert.True(t, manifest.HasColumns())
		assert.Equal(t, newColumns, manifest.GetColumns())

		// Write to file
		manifest.WriteTo(manifestPath)

		// Load stored content
		f := utils.OpenFile(manifestPath, os.O_RDONLY)
		content := utils.ReadAll(f, manifestPath)
		utils.CloseFile(f, manifestPath)

		// Parse JSON
		var parsedContent map[string]interface{}
		utils.JsonUnmarshal(content, manifestPath, &parsedContent)

		// New columns are stored in the manifest file
		assert.Equal(t, testData.output, parsedContent)
	}
}

func getTestData() []testData {
	return []testData{
		{
			input:   "",
			columns: nil,
			output: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			input:   "{}",
			columns: nil,
			output: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			input:   "{\"columns\": []}",
			columns: make([]string, 0),
			output: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			input:   "{\"columns\": [\"x\", \"y\", \"z\"]}",
			columns: []string{"x", "y", "z"},
			output: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			input:   "{\"foo\": \"bar\", \"columns\": [\"x\", \"y\", \"z\"]}",
			columns: []string{"x", "y", "z"},
			// foo:bar from the original manifest is preserved and new columns are set
			output: map[string]interface{}{
				"foo":     "bar", // from original manifest
				"columns": []interface{}{"1", "2", "3"},
			},
		},
	}
}
