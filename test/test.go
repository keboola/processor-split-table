package test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/juju/fslock"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
)

// GetDataDirs returns list of all dataDir tests in the root directory.
func GetDataDirs(t *testing.T, root string) []string {
	t.Helper()

	var dirs []string

	// Iterate over directory structure
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == root {
			return nil
		}

		// Skip hidden files
		if strings.HasPrefix(filepath.Base(path), ".") {
			return nil
		}

		// Skip sub-directories
		if info.IsDir() {
			dirs = append(dirs, path)
			return fs.SkipDir
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return dirs
}

// GetFileContent or default value.
func GetFileContent(t *testing.T, path string, def string) (exists bool, content string) {
	t.Helper()

	// Return default value if file not exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, def
	}

	// Read content, handle error
	contentBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf(fmt.Sprint(err))
	}

	return true, string(contentBytes)
}

// CompileBinary compiles component to binary used in this test.
func CompileBinary(t *testing.T, entrypointDir string, tempDir string) string {
	t.Helper()

	// Prevent parallel compilation, it doesn't work with the Go cache is empty
	l := fslock.New(filepath.Join(os.TempDir(), "split-processor-compilation.lock"))
	require.NoError(t, l.Lock())
	defer func() {
		require.NoError(t, l.Unlock())
	}()

	// Run build command
	var stdout, stderr bytes.Buffer
	binaryPath := tempDir + "/bin_data_dir_tests"
	cmd := exec.Command("go", "build", "-o", binaryPath, entrypointDir)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		t.Fatalf("Compilation failed: %s\n%s\n", stdout.Bytes(), stderr.Bytes())
	}

	return binaryPath
}

// WildcardToRegexp converts expected stdout/stderr
// with wildcards to regexp used in the assert.
func WildcardToRegexp(pattern string) string {
	var result strings.Builder
	for i, literal := range strings.Split(pattern, "*") {
		// Replace * with .*
		if i > 0 {
			result.WriteString(".*")
		}

		// Quote any regular expression meta characters in the text
		result.WriteString(regexp.QuoteMeta(literal))
	}
	return result.String()
}

func GzipAllInDir(t *testing.T, dir string) {
	t.Helper()

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, entryErr error) error {
		// Stop on error
		if entryErr != nil {
			return entryErr
		}

		if !d.IsDir() && strings.HasSuffix(path, ".ungzipped") {
			GzipFile(t, path)
		}

		return nil
	})
	require.NoError(t, err)
}

func UnGzipAllInDir(t *testing.T, dir string) {
	t.Helper()

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, entryErr error) error {
		// Stop on error
		if entryErr != nil {
			return entryErr
		}

		if !d.IsDir() && strings.HasSuffix(path, kbc.GzipFileExtension) {
			UnGzipFile(t, path)
		}

		return nil
	})
	require.NoError(t, err)
}

func GzipFile(t *testing.T, srcPath string) {
	t.Helper()

	trgPath := strings.TrimSuffix(srcPath, ".ungzipped")

	// Open file
	in, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
	require.NoError(t, err)

	// Open target
	out, err := os.OpenFile(trgPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, kbc.NewFilePermissions)
	require.NoError(t, err)

	// create gzip reader
	wgz := gzip.NewWriter(out)

	// Compress source content
	_, err = io.Copy(wgz, in)
	require.NoError(t, err)

	// Close all
	require.NoError(t, wgz.Close())
	require.NoError(t, in.Close())
	require.NoError(t, out.Close())

	// Remove original file
	require.NoError(t, os.Remove(srcPath))
}

func UnGzipFile(t *testing.T, srcPath string) {
	t.Helper()

	trgPath := srcPath + ".ungzipped"

	// Open file
	in, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
	require.NoError(t, err)

	// create gzip reader
	rgz, err := gzip.NewReader(in)
	require.NoError(t, err)

	// Open target
	out, err := os.OpenFile(trgPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, kbc.NewFilePermissions)
	require.NoError(t, err)

	// Decompress source content
	_, err = io.Copy(out, rgz) // nolint:gosec
	require.NoError(t, err)

	// Close all
	require.NoError(t, rgz.Close())
	require.NoError(t, in.Close())
	require.NoError(t, out.Close())

	// Remove original file
	require.NoError(t, os.Remove(srcPath))
}
