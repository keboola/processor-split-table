package slicedWriter

import (
	"bufio"
	"keboola.processor-split-table/src/config"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/utils"
	"os"
)

const OutBufferSize = 20 * 1024 * 1024 // 20MB

// slice writes to the one slice
type slice struct {
	mode     config.Mode
	maxBytes uint64
	maxRows  uint64
	path     string
	file     *os.File
	buffer   *bufio.Writer
	rows     uint64
	bytes    uint64
}

func NewSlice(c *config.Config, filePath string) *slice {
	file := utils.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	bufWriter := bufio.NewWriterSize(file, OutBufferSize)
	return &slice{
		c.Parameters.Mode,
		c.Parameters.BytesPerSlice,
		c.Parameters.RowsPerSlice,
		filePath,
		file,
		bufWriter,
		0,
		0,
	}
}

func (s *slice) Write(row []byte, rowLength uint64) {
	_, err := s.buffer.Write(row)
	if err != nil {
		kbc.PanicApplicationError("Cannot write row to slice \"%s\": %s", s.path, err)
	}
	s.rows++
	s.bytes += rowLength
}

func (s *slice) Close() {
	err := s.buffer.Flush()
	if err != nil {
		kbc.PanicApplicationError("Cannot flush buffer when closing slice \"%s\".", s.path)
	}

	err = s.file.Close()
	if err != nil {
		kbc.PanicApplicationError("Cannot close file when closing slice \"%s\".", s.path)
	}
}

func (s *slice) IsSpaceForNextRow(rowLength uint64) bool {
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
