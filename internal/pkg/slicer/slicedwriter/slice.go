package slicedwriter

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"

	gzip "github.com/klauspost/pgzip"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/processor/config"
)

const (
	OutBufferSize = 20 * 1024 * 1024  // 20 MB
	GcMaxBytes    = 500 * 1024 * 1024 // run garbage collector each 500 MB written
)

// slice writes to the one slice.
type slice struct {
	mode        config.Mode
	maxBytes    uint64
	maxRows     uint64
	path        string
	file        *os.File
	writer      io.Writer
	rows        uint64
	bytes       uint64
	bytesFromGc uint64 // bytes from last garbage collector run
}

func newSlice(mode config.Mode, maxBytes uint64, maxRows uint64, gzipEnabled bool, gzipLevel int, filePath string) (*slice, error) {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, kbc.NewFilePermissions)
	if err != nil {
		return nil, err
	}

	// Use gzip compression?
	var writer io.Writer
	if gzipEnabled {
		writer, err = gzip.NewWriterLevel(file, gzipLevel)
		if err != nil {
			return nil, fmt.Errorf("cannot create gzip writer: %w", err)
		}
	} else {
		writer = bufio.NewWriterSize(file, OutBufferSize)
	}

	return &slice{
		mode,
		maxBytes,
		maxRows,
		filePath,
		file,
		writer,
		0,
		0,
		0,
	}, nil
}

func (s *slice) Write(row []byte, rowLength uint64) error {
	n, err := s.writer.Write(row)
	if err != nil {
		return fmt.Errorf("cannot write row to slice \"%s\": %w", s.path, err)
	}
	if n != int(rowLength) {
		return fmt.Errorf("unexpected length written to \"%s\", expected %d, written %d", s.path, rowLength, n)
	}
	s.rows++
	s.bytes += rowLength
	s.bytesFromGc += rowLength

	// Run garbage collector each GcMaxBytes
	if s.bytesFromGc > GcMaxBytes {
		runtime.GC()
		s.bytesFromGc = 0
	}

	return nil
}

func (s *slice) Close() error {
	// Close writer according to its type
	switch w := s.writer.(type) {
	case *bufio.Writer:
		err := w.Flush()
		if err != nil {
			return fmt.Errorf("cannot flush writer when closing slice \"%s\": %w", s.path, err)
		}
	case io.WriteCloser:
		err := w.Close()
		if err != nil {
			return fmt.Errorf("cannot close writer when closing slice \"%s\": %w", s.path, err)
		}
	default:
		return fmt.Errorf("unexpected writer type \"%T\"", s.writer)
	}

	// Close file
	err := s.file.Close()
	if err != nil {
		return fmt.Errorf("cannot close file when closing slice \"%s\": %w", s.path, err)
	}

	// Go runtime doesn't know maximum memory in Kubernetes/Docker, so we clean-up after each slice.
	runtime.GC()

	return nil
}

func (s *slice) IsSpaceForNextRow(rowLength uint64) bool {
	// In each slice must have at least 1 row
	if s.rows == 0 {
		return true
	}

	switch s.mode {
	case config.ModeBytes:
		return s.bytes+rowLength <= s.maxBytes
	case config.ModeRows:
		return s.rows < s.maxRows
	default:
		panic(fmt.Errorf("unexpected sliced writer mode \"%v\"", s.mode))
	}
}
