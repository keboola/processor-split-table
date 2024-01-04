package manifest

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
)

type testData struct {
	comment    string
	input      string
	columns    []string
	hasColumns bool
	enclosure  byte
	delimiter  byte
	newState   map[string]interface{}
}

func TestGetSet(t *testing.T) {
	t.Parallel()

	for _, testData := range getTestData() {
		// Create temp dir
		tempDir := t.TempDir()

		// Create test manifest.json
		manifestPath := tempDir + "/manifest.json"
		if testData.input != "" {
			require.NoError(t, os.WriteFile(manifestPath, []byte(testData.input), kbc.NewFilePermissions))
		}

		// Check expected columns
		manifest, err := LoadManifest(manifestPath)
		require.NoError(t, err)
		assert.Equal(t, testData.columns, manifest.Columns())
		assert.Equal(t, testData.hasColumns, manifest.HasColumns())

		// Set different columns
		newColumns := []string{"1", "2", "3"}
		manifest.SetColumns(newColumns)
		assert.True(t, manifest.HasColumns())
		assert.Equal(t, newColumns, manifest.Columns())

		// Write to file
		require.NoError(t, manifest.WriteTo(manifestPath))

		// Load stored content
		content, err := os.ReadFile(manifestPath)
		require.NoError(t, err)

		// Parse JSON
		var parsedContent map[string]interface{}
		require.NoError(t, json.Unmarshal(content, &parsedContent))

		// New columns are stored in the manifest file
		assert.Equal(t, testData.newState, parsedContent)
	}
}

func getTestData() []testData {
	return []testData{
		{
			comment:    "file not exists",
			input:      "",
			columns:    nil,
			hasColumns: false,
			enclosure:  '"',
			delimiter:  ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:    "empty json",
			input:      "{}",
			columns:    nil,
			hasColumns: false,
			enclosure:  '"',
			delimiter:  ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:    "empty columns",
			input:      "{\"columns\": []}",
			columns:    make([]string, 0),
			hasColumns: false,
			enclosure:  '"',
			delimiter:  ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:    "columns defined, overwritten by setter",
			input:      "{\"columns\": [\"x\", \"y\", \"z\"]}",
			columns:    []string{"x", "y", "z"},
			hasColumns: true,
			enclosure:  '"',
			delimiter:  ',',
			newState: map[string]interface{}{
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:    "columns defined, overwritten by setter + original value preserved",
			input:      "{\"foo\": \"bar\", \"columns\": [\"x\", \"y\", \"z\"]}",
			columns:    []string{"x", "y", "z"},
			hasColumns: true,
			enclosure:  '"',
			delimiter:  ',',
			// foo:bar from the original manifest is preserved and new columns are set
			newState: map[string]interface{}{
				"foo":     "bar", // from original manifest
				"columns": []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:    "custom delimiter",
			input:      "{\"delimiter\": \"\\t\"}",
			columns:    nil,
			hasColumns: false,
			enclosure:  '"',
			delimiter:  '\t',
			newState: map[string]interface{}{
				"delimiter": "\t",
				"columns":   []interface{}{"1", "2", "3"},
			},
		},
		{
			comment:    "custom enclosure",
			input:      "{\"enclosure\": \"'\"}",
			columns:    nil,
			hasColumns: false,
			enclosure:  '"',
			delimiter:  '\t',
			newState: map[string]interface{}{
				"enclosure": "'",
				"columns":   []interface{}{"1", "2", "3"},
			},
		},
	}
}
