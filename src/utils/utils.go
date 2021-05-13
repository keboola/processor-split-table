package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/otiai10/copy"
	"io/ioutil"
	"keboola.processor-split-table/src/kbc"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func Mkdir(path string) {
	if err := os.Mkdir(path, 0755); err != nil && !os.IsExist(err) {
		kbc.PanicApplicationError("Cannot create dir \"%s\": %s", path, err)
	}
}

// CopyRecursive dir or file
func CopyRecursive(src string, target string) {
	if err := copy.Copy(src, target); err != nil {
		kbc.PanicApplicationError("Copy \"%s\" -> \"%s\" error: %s", src, target, err)
	}
}

func OpenFile(path string, flag int) *os.File {
	file, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		kbc.PanicApplicationError("Cannot open file \"%s\": %s", path, err)
	}
	return file
}

func CloseFile(file *os.File, path string) {
	if file == nil {
		return
	}

	if err := file.Sync(); err != nil {
		kbc.PanicApplicationError("Cannot sync file \"%s\": %s", path, err)
	}

	if err := file.Close(); err != nil {
		kbc.PanicApplicationError("Cannot close file \"%s\": %s", path, err)
	}
}

// FileExists returns true if file exists.
func FileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		kbc.PanicApplicationError("Cannot test if file exists \"%s\": %s", path, err)
	}

	return false
}

func WriteStringToFile(file *os.File, str string, path string) {
	WriteToFile(file, []byte(str), path)
}

func WriteToFile(file *os.File, str []byte, path string) {
	if _, err := file.Write(str); err != nil {
		kbc.PanicApplicationError("Cannot write to file \"%s\": %s", path, err)
	}
}

func FlushWriter(writer *bufio.Writer, path string) {
	if writer != nil {
		if err := writer.Flush(); err != nil {
			kbc.PanicApplicationError("Cannot flush file \"%s\": %s", path, err)
		}
	}
}

func JsonUnmarshal(data []byte, path string, v interface{}) {
	jsonErr := json.Unmarshal(data, v)
	if jsonErr != nil {
		kbc.PanicUserError("Cannot parse JSON file \"%s\": %s", path, jsonErr)
	}
}

func ReadAllFromFile(file *os.File, path string) []byte {
	content, err := ioutil.ReadAll(file)
	if err != nil {
		kbc.PanicApplicationError("Cannot read file \"%s\": %s", path, err)
	}
	return content
}

// AssertDirectoryContentsSame compares two directories using diff command.
func AssertDirectoryContentsSame(t *testing.T, expectedDir string, dataDir string) {
	// Prepare diff command
	expectedDirAbs, _ := filepath.Abs(expectedDir)
	dataDirAbs, _ := filepath.Abs(dataDir)
	cmd := exec.Command(
		"diff",
		"--exclude=.gitkeep",
		"--ignore-all-space",
		"--recursive",
		expectedDirAbs,
		dataDirAbs,
	)

	// Store output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run diff command
	exitCode := 0
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
	}

	// If exitCode == 0 -> directories are same
	if exitCode != 0 {
		t.Fatalf(
			"Two directories are not the same:\n%s\n%s\n%s\n%s\n",
			expectedDir,
			dataDir,
			stdout.String(),
			stderr.String(),
		)
	}
}
