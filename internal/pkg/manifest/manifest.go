// Package manifest provides table manifest reading and writing.
package manifest

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

const (
	DefaultDelimiter   = ','
	EnclosureDelimiter = '"'
)

// Manifest is a parsed table manifest.
// The original content is always preserved, so it is represented as an OrderedMap.
// There is only one setter SetColumns, it is used to move the list of columns from CSV header to the manifest.
type Manifest struct {
	path     string
	exists   bool
	modified bool
	content  *orderedmap.OrderedMap // decoded JSON content

	columns   []string
	delimiter byte
	enclosure byte
}

func LoadManifest(path string) (*Manifest, error) {
	content, found, err := loadManifestContent(path)
	if err != nil {
		return nil, err
	}

	m := &Manifest{path: path, exists: found, content: content}

	// Load delimiter
	m.delimiter = DefaultDelimiter
	if val, ok := m.content.Get("delimiter"); ok {
		// Delimiter must be strings
		if str, ok := val.(string); !ok {
			return nil, kbc.UserErrorf("unexpected type \"%T\" of the manifest \"delimiter\" key", val)
		} else if len(str) != 1 {
			// Delimiter must be 1 char
			return nil, kbc.UserErrorf("unexpected length \"%d\" of the manifest \"delimiter\" key. Expected 1 char", len(str))
		} else {
			m.delimiter = str[0]
		}
	}

	// Load enclosure
	m.enclosure = EnclosureDelimiter
	if val, ok := m.content.Get("enclosure"); ok {
		// Enclosure must be strings array
		if str, ok := val.(string); !ok {
			return nil, kbc.UserErrorf("unexpected type \"%T\" of the manifest \"enclosure\" key", val)
		} else if len(str) != 1 {
			// Enclosure must be 1 char
			return nil, kbc.UserErrorf("unexpected length \"%d\" of the manifest \"enclosure\" key. Expected 1 char", len(str))
		} else {
			m.enclosure = str[0]
		}
	}

	// Load columns
	if val, ok := m.content.Get("columns"); ok {
		// Columns must be strings array
		if slice, ok := val.([]string); !ok {
			return nil, kbc.UserErrorf("unexpected type \"%T\" of the manifest \"columns\" key.", val)
		} else {
			m.columns = slice
		}
	}

	return m, nil
}

// WriteTo output directory.
func (m *Manifest) WriteTo(path string) error {
	// Encode JSON
	data, jsonErr := json.MarshalIndent(m.content, "", "    ")
	if jsonErr != nil {
		return fmt.Errorf("cannot encode manifest to JSON: %w", jsonErr)
	}

	// Write to file
	return os.WriteFile(path, data, kbc.NewFilePermissions)
}

func (m *Manifest) Exists() bool {
	return m.exists
}

func (m *Manifest) Modified() bool {
	return m.modified
}

func (m *Manifest) HasColumns() bool {
	if value, ok := m.content.Get("columns"); ok {
		if array, ok := value.([]string); ok && len(array) > 0 {
			return true
		}
	}
	return false
}

func (m *Manifest) Columns() []string {
	return m.columns
}

func (m *Manifest) SetColumns(columns []string) {
	m.content.Set("columns", columns)
	m.columns = columns
	m.modified = true
}

func (m *Manifest) Delimiter() byte {
	return m.delimiter
}

func (m *Manifest) Enclosure() byte {
	return m.enclosure
}

func loadManifestContent(path string) (content *orderedmap.OrderedMap, found bool, err error) {
	if found, err = utils.FileExists(path); err != nil {
		return nil, false, err
	} else if !found {
		// Return empty map, file will be created
		return orderedmap.New(), false, nil
	}

	// Read file
	contentBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}

	// Parse JSON
	content = orderedmap.New()
	if err := json.Unmarshal(contentBytes, content); err != nil {
		return nil, false, err
	}

	// Convert columns []interface -> []string
	if val, ok := content.Get("columns"); ok {
		if raw, ok := val.([]interface{}); ok {
			strings := make([]string, len(raw))
			for i := range raw {
				strings[i] = fmt.Sprintf("%v", raw[i])
			}
			content.Set("columns", strings)
		} else {
			return nil, false, fmt.Errorf("unexpected type \"%T\" of the manifest \"columns\" key", val)
		}
	}

	return content, true, nil
}
