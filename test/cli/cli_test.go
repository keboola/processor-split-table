package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/google/shlex"
	"github.com/joho/godotenv"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/utils"
	"github.com/keboola/processor-split-table/test"
)

// TestCLI runs all data-dir tests from the file directory.
// In "envs" and "args" fi $IN_DIR and $OUT_DIR placeholder.
func TestCLI(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	workingDir := filepath.Join(rootDir, ".out")
	require.NoError(t, os.RemoveAll(workingDir))
	require.NoError(t, os.MkdirAll(workingDir, 0o750))

	// Compile binary, it will be run in the tests
	entrypointDir := rootDir + "/../../cmd/cli"
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

	inDirSource := filepath.Join(testDir, "in")
	outDirExpected := filepath.Join(testDir, "out")
	inDir := filepath.Join(workingDir, "in")
	outDir := filepath.Join(workingDir, "out")
	require.NoError(t, os.Mkdir(outDir, 0o755))

	// Copy all from source dir to data dir
	found, err := utils.FileExists(inDirSource)
	require.NoError(t, err)
	if found {
		err := copy.Copy(inDirSource, inDir)
		if err != nil {
			t.Fatalf("Copy error: " + fmt.Sprint(err))
		}
	}

	// Load arguments
	var args []string
	{
		content, err := os.ReadFile(filepath.Join(testDir, "args"))
		if !errors.Is(err, os.ErrNotExist) {
			require.NoError(t, err)
			content = bytes.ReplaceAll(content, []byte("$IN_DIR"), []byte(inDir))
			content = bytes.ReplaceAll(content, []byte("$OUT_DIR"), []byte(outDir))
			args, err = shlex.Split(string(content))
			require.NoError(t, err)
		}
	}

	// Load environment variables
	var envs []string
	{
		content, err := os.ReadFile(filepath.Join(testDir, "envs"))
		if !errors.Is(err, os.ErrNotExist) {
			require.NoError(t, err)
			content = bytes.ReplaceAll(content, []byte("$IN_DIR"), []byte(inDir))
			content = bytes.ReplaceAll(content, []byte("$OUT_DIR"), []byte(outDir))
			envsMap, err := godotenv.UnmarshalBytes(content)
			require.NoError(t, err)
			for k, v := range envsMap {
				envs = append(envs, fmt.Sprintf(`%s=%s`, k, v))
			}
		}
	}

	// Gzip files for easier definition
	test.GzipAllInDir(t, inDir)

	// Prepare command
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary)
	cmd.Dir = outDir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Args = args
	cmd.Env = append(cmd.Env, envs...)

	// Run command
	exitCode := 0
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
	}

	// Un-gzip files for easier comparison
	test.UnGzipAllInDir(t, outDir)

	AssertExpectations(t, testDir, outDirExpected, outDir, exitCode, stdout.String(), stderr.String())
}

// AssertExpectations compares expectations with the actual state.
func AssertExpectations(
	t *testing.T,
	testDir string,
	outDirExpected string,
	outDir string,
	exitCode int,
	stdout string,
	stderr string,
) {
	t.Helper()

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
	found, err := utils.FileExists(outDirExpected)
	require.NoError(t, err)
	if found {
		utils.AssertDirectoryContentsSame(t, outDirExpected, outDir)
	}
}
