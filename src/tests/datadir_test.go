package tests

import (
	"bytes"
	"fmt"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"io/fs"
	"io/ioutil"
	"keboola.processor-split-by-nrows/src/utils"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestDataDirs runs all data-dir tests from the file directory.
func TestDataDirs(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	// Create temp dir
	tempDir := t.TempDir()

	// Compile binary, it will be run in the tests
	srcDir := rootDir + "/.."
	binary := CompileBinary(t, srcDir, tempDir)

	// Run binary in each data dir
	for _, testDir := range GetDataDirs(t, rootDir) {
		// Run test for each directory
		t.Run(filepath.Base(testDir), func(t *testing.T) {
			RunDataDirTest(t, testDir, binary)
		})
	}
}

// RunDataDirTest runs one data-dir test.
func RunDataDirTest(t *testing.T, testDir string, binary string) {
	// Create runtime data dir
	dataDir := t.TempDir()

	// Copy all from source dir to data dir
	sourceDir := testDir + "/source/data"
	if utils.FileExists(sourceDir) {
		err := copy.Copy(sourceDir, dataDir)
		if err != nil {
			t.Fatalf("Copy error: " + fmt.Sprint(err))
		}
	}

	// Create common directories
	_ = os.Mkdir(dataDir+"/out", 0644)
	_ = os.Mkdir(dataDir+"/out/tables", 0644)
	_ = os.Mkdir(dataDir+"/out/files", 0644)
	_ = os.Mkdir(dataDir+"/in", 0644)
	_ = os.Mkdir(dataDir+"/in/tables", 0644)
	_ = os.Mkdir(dataDir+"/in/files", 0644)

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
		if len(expectedStderr) == 0 {
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
	if utils.FileExists(expectedDir) {
		utils.AssertDirectoryContentsSame(t, expectedDir, dataDir+"/out")
	}
}

// GetFileContent or default value.
func GetFileContent(t *testing.T, path string, def string) (exists bool, content string) {
	// Return default value if file not exists
	if _, err := os.Stat(path); os.IsNotExist(err) == true {
		return false, def
	}

	// Read content, handle error
	contentBytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf(fmt.Sprint(err))
	}

	return true, string(contentBytes)
}

// GetDataDirs returns list of all dataDir tests in the root directory.
func GetDataDirs(t *testing.T, root string) []string {
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

// CompileBinary compiles component to binary used in this test
func CompileBinary(t *testing.T, srcDir string, tempDir string) string {
	var stdout, stderr bytes.Buffer
	binaryPath := tempDir + "/bin_data_dir_tests"
	cmd := exec.Command("go", "build", "-o", binaryPath, srcDir)
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
