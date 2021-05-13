package manifest

import (
	"encoding/json"
	"fmt"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/utils"
	"os"
)

type Manifest struct {
	path    string
	content map[string]interface{} // decoded JSON content
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
	if _, ok := m.content["columns"]; ok {
		return true
	}

	return false
}

func (m *Manifest) GetColumns() []string {
	if val, ok := m.content["columns"]; ok {
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
	m.content["columns"] = columns
}

func loadManifestContent(path string) map[string]interface{} {
	if !utils.FileExists(path) {
		// Return empty map, file will be created
		return make(map[string]interface{})
	}

	// Open file
	jsonFile := utils.OpenFile(path, os.O_RDWR)
	defer utils.CloseFile(jsonFile, path)

	// Parse JSON
	var content map[string]interface{}
	contentStr := utils.ReadAllFromFile(jsonFile, path)
	utils.JsonUnmarshal(contentStr, path, &content)

	// Convert columns []interface -> []string
	if val, ok := content["columns"]; ok {
		if raw, ok := val.([]interface{}); ok {
			strings := make([]string, len(raw))
			for i := range raw {
				strings[i] = fmt.Sprintf("%v", raw[i])
			}
			content["columns"] = strings
		} else {
			kbc.PanicApplicationError("Unexpected type \"%T\" of the manifest \"columns\" key.", val)
		}
	}

	return content
}
