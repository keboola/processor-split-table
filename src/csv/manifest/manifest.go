package manifest

import (
	"encoding/json"
	"fmt"
	"github.com/iancoleman/orderedmap"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/utils"
	"os"
)

type Manifest struct {
	path    string
	content *orderedmap.OrderedMap // decoded JSON content
}

func LoadManifest(path string) *Manifest {
	return &Manifest{path, loadManifestContent(path)}
}

// WriteTo output directory
func (m *Manifest) WriteTo(path string) {
	// Encode JSON
	data, jsonErr := json.MarshalIndent(m.content, "", "    ")
	if jsonErr != nil {
		kbc.PanicApplicationError("Cannot encode to JSON: %s", jsonErr)
	}

	// Write to file
	f := utils.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	utils.WriteToFile(f, data, path)
	utils.CloseFile(f, path)
}

func (m *Manifest) HasColumns() bool {
	if _, ok := m.content.Get("columns"); ok {
		return true
	}

	return false
}

func (m *Manifest) GetColumns() []string {
	if val, ok := m.content.Get("columns"); ok {
		// Columns must be strings array
		if slice, ok := val.([]string); ok {
			return slice
		} else {
			kbc.PanicApplicationError("Unexpected type \"%T\" of the manifest \"columns\" key.", val)
		}
	}
	return nil
}

func (m *Manifest) SetColumns(columns []string) {
	m.content.Set("columns", columns)
}

func (m *Manifest) GetDelimiter() byte {
	if val, ok := m.content.Get("delimiter"); ok {
		// Delimiter must be strings
		if val, ok := val.(string); ok {
			// Delimiter must be 1 char
			if len(val) != 1 {
				kbc.PanicUserError("Unexpected length \"%d\" of the manifest \"delimiter\" key. Expected 1 char.", len(val))
			}
			return val[0]
		} else {
			kbc.PanicUserError("Unexpected type \"%T\" of the manifest \"delimiter\" key.", val)
		}
	}

	// Default value
	return ','
}

func (m *Manifest) GetEnclosure() byte {
	if val, ok := m.content.Get("enclosure"); ok {
		// Enclosure must be strings array
		if val, ok := val.(string); ok {
			// Enclosure must be 1 char
			if len(val) != 1 {
				kbc.PanicUserError("Unexpected length \"%d\" of the manifest \"enclosure\" key. Expected 1 char.", len(val))
			}
			return val[0]
		} else {
			kbc.PanicUserError("Unexpected type \"%T\" of the manifest \"enclosure\" key.", val)
		}
	}

	// Default value
	return '"'
}

func loadManifestContent(path string) *orderedmap.OrderedMap {
	if !utils.FileExists(path) {
		// Return empty map, file will be created
		return orderedmap.New()
	}

	// Open file
	jsonFile := utils.OpenFile(path, os.O_RDWR)
	defer utils.CloseFile(jsonFile, path)

	// Parse JSON
	content := orderedmap.New()
	contentStr := utils.ReadAll(jsonFile, path)
	utils.JsonUnmarshal(contentStr, path, &content)

	// Convert columns []interface -> []string
	if val, ok := content.Get("columns"); ok {
		if raw, ok := val.([]interface{}); ok {
			strings := make([]string, len(raw))
			for i := range raw {
				strings[i] = fmt.Sprintf("%v", raw[i])
			}
			content.Set("columns", strings)
		} else {
			kbc.PanicApplicationError("Unexpected type \"%T\" of the manifest \"columns\" key.", val)
		}
	}

	return content
}
