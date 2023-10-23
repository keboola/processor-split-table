package processor

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
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

// TestDataDirs runs all data-dir tests from the file directory.
func TestDataDirs(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	// Compile binary, it will be run in the tests
	entrypointDir := rootDir + "/../../cmd/processor"
	binary := CompileBinary(t, entrypointDir, t.TempDir())

	// Run binary in each data dir
	for _, testDir := range GetDataDirs(t, rootDir) {
		testDir := testDir
		// Run test for each directory
		t.Run(filepath.Base(testDir), func(t *testing.T) {
			t.Parallel()
			RunDataDirTest(t, testDir, binary)
		})
	}
}

// RunDataDirTest runs one data-dir test.
func RunDataDirTest(t *testing.T, testDir string, binary string) {
	t.Helper()

	// Create runtime data dir
	dataDir := t.TempDir()

	// Copy all from source dir to data dir
	sourceDir := testDir + "/source/data"
	found, err := utils.FileExists(sourceDir)
	require.NoError(t, err)
	if found {
		err := copy.Copy(sourceDir, dataDir)
		if err != nil {
			t.Fatalf("Copy error: " + fmt.Sprint(err))
		}
	}

	// Gzip files for easier definition
	gzipAllInDir(t, dataDir+"/in")

	// Create common directories
	_ = os.Mkdir(dataDir+"/out", 0o755)
	_ = os.Mkdir(dataDir+"/out/tables", 0o755)
	_ = os.Mkdir(dataDir+"/out/files", 0o755)
	_ = os.Mkdir(dataDir+"/in", 0o755)
	_ = os.Mkdir(dataDir+"/in/tables", 0o755)
	_ = os.Mkdir(dataDir+"/in/files", 0o755)

	// Prepare command
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = append(cmd.Env, "KBC_DATADIR="+dataDir)

	// Run command
	exitCode := 0
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
	}

	// Un-gzip files for easier comparison
	unGzipAllInDir(t, dataDir+"/out")

	AssertExpectations(t, testDir, dataDir, exitCode, stdout.String(), stderr.String())
}

// AssertExpectations compares expectations with the actual state.
func AssertExpectations(
	t *testing.T,
	testDir string,
	dataDir string,
	exitCode int,
	stdout string,
	stderr string,
) {
	t.Helper()

	expectedDir := testDir + "/expected/data/out"
	hasExpectedStdout, expectedStdout := GetFileContent(t, testDir+"/expected-stdout", "")
	hasExpectedStderr, expectedStderr := GetFileContent(t, testDir+"/expected-stderr", "")

	// Assert exit code
	_, expectedCodeStr := GetFileContent(t, testDir+"/expected-code", "0")
	expectedCode, _ := strconv.ParseInt(strings.TrimSpace(expectedCodeStr), 10, 32)
	assert.Equal(
		t,
		int(expectedCode),
		exitCode,
		"Unexpected exit code.\nSTDOUT:\n%s\n\nSTDERR:\n%s\n\n",
		stdout,
		stderr,
	)

	// Assert STDOUT
	if hasExpectedStdout {
		if len(expectedStdout) == 0 {
			assert.Equal(t, expectedStdout, stdout, "Unexpected STDOUT.")
		} else {
			assert.Regexp(
				t,
				WildcardToRegexp(strings.TrimSpace(expectedStdout)),
				stdout,
				"Unexpected STDOUT.",
			)
		}
	}

	// Assert STDERR
	if hasExpectedStderr {
		if len(expectedStderr) == 0 {
			assert.Equal(t, expectedStderr, stderr, "Unexpected STDERR.")
		} else {
			assert.Regexp(
				t,
				WildcardToRegexp(strings.TrimSpace(expectedStderr)),
				stderr,
				"Unexpected STDERR.",
			)
		}
	}

	// Assert dirs have same content
	found, err := utils.FileExists(expectedDir)
	require.NoError(t, err)
	if found {
		utils.AssertDirectoryContentsSame(t, expectedDir, dataDir+"/out")
	}
}

// GetFileContent or default value.
func GetFileContent(t *testing.T, path string, def string) (exists bool, content string) {
	t.Helper()

	// Return default value if file not exists
	if _, err := os.Stat(path); os.IsNotExist(err) == true {
		return false, def
	}

	// Read content, handle error
	contentBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf(fmt.Sprint(err))
	}

	return true, string(contentBytes)
}

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

// CompileBinary compiles component to binary used in this test.
func CompileBinary(t *testing.T, entrypointDir string, tempDir string) string {
	t.Helper()

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

func gzipAllInDir(t *testing.T, dir string) {
	t.Helper()

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, entryErr error) error {
		// Stop on error
		if entryErr != nil {
			return entryErr
		}

		if !d.IsDir() && strings.HasSuffix(path, ".ungzipped") {
			gzipFile(t, path)
		}

		return nil
	})
	require.NoError(t, err)
}

func unGzipAllInDir(t *testing.T, dir string) {
	t.Helper()

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, entryErr error) error {
		// Stop on error
		if entryErr != nil {
			return entryErr
		}

		if !d.IsDir() && strings.HasSuffix(path, kbc.GzipFileExtension) {
			unGzipFile(t, path)
		}

		return nil
	})
	require.NoError(t, err)
}

func gzipFile(t *testing.T, srcPath string) {
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

func unGzipFile(t *testing.T, srcPath string) {
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
	_, err = io.Copy(out, rgz)
	require.NoError(t, err)

	// Close all
	require.NoError(t, rgz.Close())
	require.NoError(t, in.Close())
	require.NoError(t, out.Close())

	// Remove original file
	require.NoError(t, os.Remove(srcPath))
}
