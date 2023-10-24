package utils

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/otiai10/copy"
)

// Mkdir creates dir if not exists.
func Mkdir(path string) error {
	if err := os.Mkdir(path, 0o755); err != nil && !os.IsExist(err) {
		return fmt.Errorf("cannot create dir \"%s\": %w", path, err)
	}
	return nil
}

// CopyRecursive dir or file.
func CopyRecursive(src string, target string) error {
	if err := copy.Copy(src, target); err != nil {
		return fmt.Errorf("copy \"%s\" -> \"%s\" error: %w", src, target, err)
	}
	return nil
}

// FileExists returns true if file exists.
func FileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("cannot test if file exists \"%s\": %w", path, err)
	}
	return false, nil
}

func DirSize(path string) (datasize.ByteSize, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("cannot get dir \"%s\" size: %w", path, err)
	}

	return datasize.ByteSize(size), nil
}

func RemoveSpaces(s string) string {
	return strings.ReplaceAll(s, " ", "")
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
