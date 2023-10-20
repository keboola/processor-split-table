package finder

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type FileType int

const (
	CsvTableSingle FileType = iota
	CsvTableSliced
	Directory
	File
)

type FileNode struct {
	FileType     FileType
	RelativePath string // relative path from the /data dir
	ManifestPath string // for FileType = CsvTableSingle, set even if the file does not exist
}

// FindFilesRecursive returns all files/dirs in the rootDir and sub-dirs.
// Each entry is mapped to FileNode. FileNode.FileType determines further work.
func FindFilesRecursive(rootDir string) ([]*FileNode, error) {
	// Found nodes
	var files []*FileNode

	// Manifests are processed together with the table.
	// Therefore, we need to know that we processed them.
	manifests := make(map[string]bool)

	// Iterate over directory structure
	err := filepath.WalkDir(rootDir, func(path string, entry os.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Root dir -> no operation
		if rootDir == path {
			return nil
		}

		// Skip hidden files/dirs
		if entry.Name()[0] == '.' {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		// Map entry to FileNode
		var node FileNode
		node.RelativePath = strings.TrimPrefix(path, rootDir+"/")

		// Detect type of the node
		if isSingleCsvTable(entry, node.RelativePath) {
			node.FileType = CsvTableSingle
			node.ManifestPath = node.RelativePath + ".manifest"
			manifests[node.ManifestPath] = true
		} else if isSlicedCsvTable(entry, node.RelativePath) {
			node.FileType = CsvTableSliced
			node.ManifestPath = node.RelativePath + ".manifest"
			manifests[node.ManifestPath] = true
		} else if _, ok := manifests[node.RelativePath]; ok {
			// Skip manifest of the already found CSV table.
			// Entries are lexically sorted, so manifest is always processed after related CSV table.
			return nil
		} else if entry.IsDir() {
			// Directory, it should be created in OUT dir
			node.FileType = Directory
		} else {
			// File, it should be copied
			node.FileType = File
		}

		// Store found node
		files = append(files, &node)

		// Skip sub-nodes (individual CSVs) if node is a sliced CSV table
		if node.FileType == CsvTableSliced {
			return fs.SkipDir
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot iterate over directory \"%s\": %w", rootDir, err)
	}

	return files, nil
}

func isSlicedCsvTable(entry os.DirEntry, relativePath string) bool {
	return entry.IsDir() && // Is dir
		filepath.Dir(relativePath) == "tables" && // From tables dir
		hasCsvSuffix(relativePath) // With CSV suffix
}

func isSingleCsvTable(entry os.DirEntry, relativePath string) bool {
	return !entry.IsDir() && // Is file
		filepath.Dir(relativePath) == "tables" && // From tables dir
		hasCsvSuffix(relativePath) // With CSV suffix
}

func hasCsvSuffix(path string) bool {
	return strings.HasSuffix(path, ".csv")
}
