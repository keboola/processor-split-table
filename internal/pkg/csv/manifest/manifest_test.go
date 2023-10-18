package manifest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

type testData struct {
	comment   string
	input     string
	columns   []string
	enclosure byte
	delimiter byte
	newState  map[string]interface{}
}

func TestGetSet(t *testing.T) {
	t.Parallel()

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
		utils.JSONUnmarshal(content, manifestPath, &parsedContent)

		// New columns are stored in the manifest file
		assert.Equal(t, testData.newState, parsedContent)
	}
}

func getTestData() []testData {
	return []testData{
		{
			comment:   "file not exists",
			input:     "",
			columns:   nil,
			enclosure: '"',
			delimiter: ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:   "empty json",
			input:     "{}",
			columns:   nil,
			enclosure: '"',
			delimiter: ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:   "empty columns",
			input:     "{\"columns\": []}",
			columns:   make([]string, 0),
			enclosure: '"',
			delimiter: ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:   "columns defined, overwritten by setter",
			input:     "{\"columns\": [\"x\", \"y\", \"z\"]}",
			columns:   []string{"x", "y", "z"},
			enclosure: '"',
			delimiter: ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:   "columns defined, overwritten by setter + original value preserved",
			input:     "{\"foo\": \"bar\", \"columns\": [\"x\", \"y\", \"z\"]}",
			columns:   []string{"x", "y", "z"},
			enclosure: '"',
			delimiter: ',',
			// foo:bar from the original manifest is preserved and new columns are set
			newState: map[string]interface{}{
				"foo":     "bar", // from original manifest
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:   "custom delimiter",
			input:     "{\"delimiter\": \"\\t\"}",
			columns:   nil,
			enclosure: '"',
			delimiter: '\t',
			newState: map[string]interface{}{
				"delimiter": "\t",
				"columns":   []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:   "custom enclosure",
			input:     "{\"enclosure\": \"'\"}",
			columns:   nil,
			enclosure: '"',
			delimiter: '\t',
			newState: map[string]interface{}{
				"enclosure": "'",
				"columns":   []interface{}{"1", "2", "3"},
			},
		},
	}
}
