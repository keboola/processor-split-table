package kbc

import (
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/c2h5oh/datasize"
)

// Slices represents array of CSV table slices.
type Slices []Slice

// Slice is one slice from a sliced CSV table.
type Slice struct {
	fs.DirEntry
	path string
}

// FindSlices finds all slices in the dir.
// Slice is each file in the dir at the level 1.
func FindSlices(dir string) (out Slices, err error) {
	err = filepath.WalkDir(dir, func(subPath string, d fs.DirEntry, walkErr error) (err error) {
		// Stop on error
		if walkErr != nil {
			return walkErr
		}

		// Skip top dir
		if dir == subPath {
			return nil
		}

		// Handle unexpected subdirectory
		if d.IsDir() {
			return fmt.Errorf(`unexpected directory "%s"`, subPath)
		}

		// Store found slice
		out = append(out, Slice{DirEntry: d, path: subPath})
		return nil
	})

	if err != nil {
		return nil, err
	}

	return out, nil
}

func (v Slices) Paths() (out []string) {
	for _, item := range v {
		out = append(out, item.path)
	}
	return out
}

func (v Slices) Size() (size datasize.ByteSize, err error) {
	for _, item := range v {
		info, err := item.Info()
		if err != nil {
			return 0, err
		}
		size += datasize.ByteSize(info.Size())
	}
	return size, nil
}

func (v Slice) Path() string {
	return v.path
}
