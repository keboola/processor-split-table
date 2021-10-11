package slicedWriter

import (
	"bufio"
	gzip "github.com/klauspost/pgzip"
	"io"
	"keboola.processor-split-table/src/config"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/utils"
	"os"
	"runtime"
)

const OutBufferSize = 20 * 1024 * 1024 // 20 MB
const GcMaxBytes = 500 * 1024 * 1024   // run garbage collector each 500 MB written

// slice writes to the one slice
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

func NewSlice(mode config.Mode, maxBytes uint64, maxRows uint64, gzipEnabled bool, gzipLevel int, filePath string) *slice {
	file := utils.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)

	// Use gzip compression?
	var err error
	var writer io.Writer
	if gzipEnabled {
		writer, err = gzip.NewWriterLevel(file, gzipLevel)
		if err != nil {
			kbc.PanicApplicationError("Cannot create gzip writer: %s", err)
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
	}
}

func (s *slice) Write(row []byte, rowLength uint64) {
	n, err := s.writer.Write(row)
	if err != nil {
		kbc.PanicApplicationError("Cannot write row to slice \"%s\": %s", s.path, err)
	}
	if n != int(rowLength) {
		kbc.PanicApplicationError("Unexpected length written to \"%s\". Expected %d, written %d.", s.path, rowLength, n)
	}
	s.rows++
	s.bytes += rowLength
	s.bytesFromGc += rowLength

	// Run garbage collector each GcMaxBytes
	if s.bytesFromGc > GcMaxBytes {
		runtime.GC()
		s.bytesFromGc = 0
	}
}

func (s *slice) Close() {
	// Close writer according to its type
	switch w := s.writer.(type) {
	case *bufio.Writer:
		err := w.Flush()
		if err != nil {
			kbc.PanicApplicationError("Cannot flush writer when closing slice \"%s\": %s", s.path, err)
		}
	case io.WriteCloser:
		err := w.Close()
		if err != nil {
			kbc.PanicApplicationError("Cannot close writer when closing slice \"%s\": %s", s.path, err)
		}
	default:
		kbc.PanicApplicationError("Unexpected writer type \"%T\".", s.writer)
	}

	// Close file
	err := s.file.Close()
	if err != nil {
		kbc.PanicApplicationError("Cannot close file when closing slice \"%s\": %s", s.path, err)
	}

	// Go runtime doesn't know maximum memory in Kubernetes/Docker, so we clean-up after each slice.
	runtime.GC()
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
		kbc.PanicApplicationError("Unexpected sliced writer mode \"%s\".", s.mode)
		return false // unreachable
	}
}
