package finder

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"runtime"
	"testing"
)

func TestFindFilesRecursiveEmpty(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	files := FindFilesRecursive(filepath.Dir(testFile) + "/fixtures/empty")
	var expected []*FileNode
	assert.Equal(t, expected, files)
}

func TestFindFilesRecursiveComplex(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	files := FindFilesRecursive(filepath.Dir(testFile) + "/fixtures/complex")
	expected := []*FileNode{
		{
			FileType:     Directory,
			RelativePath: "files",
		},
		{
			FileType:     File,
			RelativePath: "files/foo1.bar",
		},
		{
			FileType:     File,
			RelativePath: "files/foo2.bar",
		},
		{
			FileType:     File,
			RelativePath: "files/foo3.csv",
		},
		{
			FileType:     Directory,
			RelativePath: "files/sub",
		},
		{
			FileType:     Directory,
			RelativePath: "files/sub/dir",
		},
		{
			FileType:     File,
			RelativePath: "files/sub/dir/foo4.bar",
		},
		{
			FileType:     File,
			RelativePath: "files/sub/dir/foo5.bar",
		},
		{
			FileType:     File,
			RelativePath: "files/sub/dir/foo6.csv",
		},
		{
			FileType:     Directory,
			RelativePath: "tables",
		},
		{
			FileType:     Directory,
			RelativePath: "tables/sub",
		},
		{
			FileType:     Directory,
			RelativePath: "tables/sub/dir",
		},
		{
			FileType:     File,
			RelativePath: "tables/sub/dir/foo7.bar",
		},
		{
			FileType:     File,
			RelativePath: "tables/sub/dir/foo8.csv",
		},
		{
			FileType:     CsvTableSingle, // <<<<<<<<<<<
			RelativePath: "tables/table1.csv",
			ManifestPath: "tables/table1.csv.manifest",
		},
		{
			FileType:     CsvTableSingle, // <<<<<<<<<<<
			RelativePath: "tables/table2.csv",
			ManifestPath: "tables/table2.csv.manifest",
		},
		{
			FileType:     CsvTableSliced, // <<<<<<<<<<<
			RelativePath: "tables/table3.csv",
			ManifestPath: "tables/table3.csv.manifest",
		},
		{
			FileType:     CsvTableSliced, // <<<<<<<<<<<
			RelativePath: "tables/table4.csv",
			ManifestPath: "tables/table4.csv.manifest",
		},
	}
	assert.Equal(t, expected, files)
}
