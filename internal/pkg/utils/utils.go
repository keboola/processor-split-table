package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/otiai10/copy"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
)

func Mkdir(path string) {
	if err := os.Mkdir(path, 0o755); err != nil && !os.IsExist(err) {
		kbc.PanicApplicationErrorf("Cannot create dir \"%s\": %s", path, err)
	}
}

// CopyRecursive dir or file.
func CopyRecursive(src string, target string) {
	if err := copy.Copy(src, target); err != nil {
		kbc.PanicApplicationErrorf("Copy \"%s\" -> \"%s\" error: %s", src, target, err)
	}
}

func OpenFile(path string, flag int) *os.File {
	file, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		kbc.PanicApplicationErrorf("Cannot open file \"%s\": %s", path, err)
	}
	return file
}

func CloseFile(file *os.File, path string) {
	if file == nil {
		return
	}

	if err := file.Close(); err != nil {
		kbc.PanicApplicationErrorf("Cannot close file \"%s\": %s", path, err)
	}
}

// FileExists returns true if file exists.
func FileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		kbc.PanicApplicationErrorf("Cannot test if file exists \"%s\": %s", path, err)
	}

	return false
}

func FileSize(path string) uint64 {
	fi, err := os.Stat(path)
	if err != nil {
		kbc.PanicApplicationErrorf("Cannot get file size of \"%s\": %s", path, err)
	}

	return uint64(fi.Size())
}

func DirSize(path string) uint64 {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		kbc.PanicApplicationErrorf("Cannot get dir \"%s\" size: %s", path, err)
	}

	return size
}

func WriteStringToFile(file *os.File, str string, path string) {
	WriteToFile(file, []byte(str), path)
}

func WriteToFile(file *os.File, str []byte, path string) {
	if _, err := file.Write(str); err != nil {
		kbc.PanicApplicationErrorf("Cannot write to file \"%s\": %s", path, err)
	}
}

func FlushWriter(writer *bufio.Writer, path string) {
	if writer != nil {
		if err := writer.Flush(); err != nil {
			kbc.PanicApplicationErrorf("Cannot flush file \"%s\": %s", path, err)
		}
	}
}

func JSONUnmarshal(data []byte, path string, v interface{}) {
	jsonErr := json.Unmarshal(data, v)
	if jsonErr != nil {
		kbc.PanicUserErrorf("Cannot parse JSON file \"%s\": %s", path, jsonErr)
	}
}

func ReadAll(in io.Reader, path string) []byte {
	content, err := io.ReadAll(in)
	if err != nil {
		kbc.PanicApplicationErrorf("Cannot read file \"%s\": %s", path, err)
	}
	return content
}

// AssertDirectoryContentsSame compares two directories using diff command.
func AssertDirectoryContentsSame(t *testing.T, expectedDir string, dataDir string) {
	t.Helper()

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
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
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
