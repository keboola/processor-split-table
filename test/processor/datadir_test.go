package processor

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/utils"
	"github.com/keboola/processor-split-table/test"
)

// TestProcessor runs all data-dir tests from the file directory.
func TestProcessor(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	workingDir := filepath.Join(rootDir, ".out")
	require.NoError(t, os.RemoveAll(workingDir))
	require.NoError(t, os.MkdirAll(workingDir, 0o750))

	// Compile binary, it will be run in the tests
	entrypointDir := rootDir + "/../../cmd/processor"
	binary := test.CompileBinary(t, entrypointDir, t.TempDir())

	// Run binary in each data dir
	for _, testDir := range test.GetDataDirs(t, rootDir) {
		testDir := testDir
		// Run test for each directory
		t.Run(filepath.Base(testDir), func(t *testing.T) {
			t.Parallel()
			RunDataDirTest(t, workingDir, testDir, binary)
		})
	}
}

// RunDataDirTest runs one data-dir test.
func RunDataDirTest(t *testing.T, workingDir, testDir string, binary string) {
	t.Helper()

	workingDir = filepath.Join(workingDir, filepath.Base(testDir))
	require.NoError(t, os.Mkdir(workingDir, 0o750))

	// Copy all from source dir to data dir
	sourceDir := testDir + "/source/data"
	found, err := utils.FileExists(sourceDir)
	require.NoError(t, err)
	if found {
		err := copy.Copy(sourceDir, workingDir)
		if err != nil {
			t.Fatalf("Copy error: " + fmt.Sprint(err))
		}
	}

	// Gzip files for easier definition
	test.GzipAllInDir(t, workingDir+"/in")

	// Create common directories
	_ = os.Mkdir(workingDir+"/out", 0o755)
	_ = os.Mkdir(workingDir+"/out/tables", 0o755)
	_ = os.Mkdir(workingDir+"/out/files", 0o755)
	_ = os.Mkdir(workingDir+"/in", 0o755)
	_ = os.Mkdir(workingDir+"/in/tables", 0o755)
	_ = os.Mkdir(workingDir+"/in/files", 0o755)

	// Prepare command
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = append(cmd.Env, "KBC_DATADIR="+workingDir)

	// Run command
	exitCode := 0
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
	}

	// Un-gzip files for easier comparison
	test.UnGzipAllInDir(t, workingDir+"/out")

	AssertExpectations(t, testDir, workingDir, exitCode, stdout.String(), stderr.String())
}

// AssertExpectations compares expectations with the actual state.
func AssertExpectations(
	t *testing.T,
	testDir string,
	workingDir string,
	exitCode int,
	stdout string,
	stderr string,
) {
	t.Helper()

	expectedDir := testDir + "/expected/data/out"
	hasExpectedStdout, expectedStdout := test.GetFileContent(t, testDir+"/expected-stdout", "")
	hasExpectedStderr, expectedStderr := test.GetFileContent(t, testDir+"/expected-stderr", "")

	// Assert exit code
	_, expectedCodeStr := test.GetFileContent(t, testDir+"/expected-code", "0")
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
				test.WildcardToRegexp(strings.TrimSpace(expectedStdout)),
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
				test.WildcardToRegexp(strings.TrimSpace(expectedStderr)),
				stderr,
				"Unexpected STDERR.",
			)
		}
	}

	// Assert dirs have same content
	found, err := utils.FileExists(expectedDir)
	require.NoError(t, err)
	if found {
		utils.AssertDirectoryContentsSame(t, expectedDir, workingDir+"/out")
	}
}
