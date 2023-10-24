package slicedwriter

import (
	"fmt"
	"io"
	"os"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/closer"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

const (
	OutBufferSize = 20 * 1024 * 1024 // 20 MB
)

// slice writes to the one slice.
type slice struct {
	writer *Writer

	path        string
	rows        uint64
	bytes       datasize.ByteSize
	bytesFromGc datasize.ByteSize // bytes from last garbage collector run

	out     io.Writer
	closers closer.Closers
}

func (w *Writer) newSlice(path string) (*slice, error) {
	s := &slice{writer: w, path: path}

	// Open the file for writing
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, kbc.NewFilePermissions)
	if err != nil {
		return nil, err
	}
	s.closers = append(s.closers, func() error {
		return file.Close()
	})

	// Add gzip compression
	if w.config.Gzip {
		if gzipWriter, err := w.gzipWriters.WriterTo(file); err == nil {
			s.out = gzipWriter
			s.closers.
				Append(func() error {
					w.gzipWriters.Put(gzipWriter)
					return nil
				}).
				Append(func() error {
					return gzipWriter.Close()
				})
		} else {
			return nil, fmt.Errorf("cannot create gzip writer: %w", err)
		}
	} else {
		bufferWriter := w.bufferWriters.WriterTo(file)
		s.out = bufferWriter
		s.closers.Append(func() error {
			w.bufferWriters.Put(bufferWriter)
			return nil
		})
		s.closers.Append(func() error {
			return bufferWriter.Flush()
		})
	}

	return s, nil
}

func (s *slice) Write(row []byte, rowLength uint64) error {
	n, err := s.out.Write(row)
	if err != nil {
		return fmt.Errorf("cannot write row to slice \"%s\": %w", s.path, err)
	}
	if n != int(rowLength) {
		return fmt.Errorf("unexpected length written to \"%s\", expected %d, written %d", s.path, rowLength, n)
	}
	s.rows++
	s.bytes += datasize.ByteSize(rowLength)
	s.bytesFromGc += datasize.ByteSize(rowLength)
	return nil
}

func (s *slice) Close() error {
	return s.closers.Close()
}

func (s *slice) IsSpaceForNextRow(rowLength uint64) bool {
	// In each slice must have at least 1 row
	if s.rows == 0 {
		return true
	}

	switch s.writer.config.Mode {
	case config.ModeBytes:
		return s.bytes+datasize.ByteSize(rowLength) <= s.writer.config.BytesPerSlice
	case config.ModeRows:
		return s.rows < s.writer.config.RowsPerSlice
	default:
		panic(fmt.Errorf("unexpected sliced writer mode \"%v\"", s.writer.config.Mode))
	}
}
