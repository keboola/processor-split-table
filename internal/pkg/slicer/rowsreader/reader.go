package rowsreader

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/pool"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/closer"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/columnsparser"
)

const (
	StartTokenBufferSize = 512 * 1024       // 512kB, initial size of buffer, it is auto-scaled
	MaxTokenBufferSize   = 50 * 1024 * 1024 // 50MB, max size of buffer -> max size of one row
)

// Reader reads rows from the CSV table.
// When slicing, we do not need to decode the individual columns, we just need to reliably determine the rows.
// Therefore, this own/fast implementation.
type Reader struct {
	config    config.Config
	path      string
	slices    []string
	sliced    bool
	delimiter byte
	enclosure byte

	rowCounter uint64

	closer      io.Closer
	scanner     *bufio.Scanner
	gzipReaders *pool.GZIPReaderPool
}

// NewSlicesReader creates the Reader for a sliced CSV table.
func NewSlicesReader(cfg config.Config, path string, slices kbc.Slices, delimiter byte, enclosure byte) (*Reader, error) {
	return newReader(cfg, path, slices.Paths(), true, delimiter, enclosure)
}

// NewFileReader creates the Reader for a single CSV file.
// It is special case of the slices reader with only one slice.
func NewFileReader(cfg config.Config, path string, delimiter byte, enclosure byte) (*Reader, error) {
	return newReader(cfg, path, []string{path}, false, delimiter, enclosure)
}

func newReader(cfg config.Config, path string, slices []string, sliced bool, delimiter byte, enclosure byte) (*Reader, error) {
	reader := &Reader{
		config:       cfg,
		path:         path,
		delimiter:   delimiter,
		enclosure:   enclosure,
		sliced:      sliced,
		gzipReaders: pool.GZIPReaders(),
	}

	// Create pipe to merge content of the slices
	pipeOut, pipeIn := io.Pipe()
	reader.closer = pipeOut
	go reader.readSlicesTo(pipeIn)

	// Create scanner with custom split function
	reader.scanner = bufio.NewScanner(pipeOut)
	reader.scanner.Split(getSplitRowsFunc(enclosure))
	reader.scanner.Buffer(make([]byte, StartTokenBufferSize), MaxTokenBufferSize)
	return reader, nil
}

func (r *Reader) Slices() uint32 {
	return uint32(len(r.slices))
}

func (r *Reader) Header() ([]string, error) {
	// The method can be used only with non-sliced input
	if r.sliced {
		return nil, fmt.Errorf(
			`the header cannot be read from the sliced file "%s", the header should be present in the manifest`,
			filepath.Base(r.path),
		)
	}

	// Header can only be read if no row has been read yet
	if r.rowCounter != 0 {
		return nil, fmt.Errorf(
			`the header cannot be read, other lines have already been read from CSV "%s"`,
			filepath.Base(r.path),
		)
	}

	// Header must be present for tables that don't have columns in manifest.json
	if !r.Read() {
		return nil, kbc.UserErrorf("missing header row in CSV \"%s\"", filepath.Base(r.path))
	}

	// Parse columns
	columns, err := columnsparser.NewParser(r.delimiter, r.enclosure).Parse(r.Bytes())
	if err != nil {
		return nil, fmt.Errorf("cannot parse CSV header: %w", err)
	}

	return columns, nil
}

func (r *Reader) Read() bool {
	ok := r.scanner.Scan()
	if ok {
		r.rowCounter++
	}

	return ok
}

func (r *Reader) Bytes() []byte {
	return r.scanner.Bytes()
}

func (r *Reader) Close() error {
	if err := r.closer.Close(); err != nil {
		return err
	}

	if err := r.scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (r *Reader) readSlicesTo(pipeIn *io.PipeWriter) {
	var err error
	for _, path := range r.slices {
		err = r.readSlice(path, pipeIn)
		if err != nil {
			break
		}
	}

	// Note: error is processed on the reading side
	_ = pipeIn.CloseWithError(err)
}

func (r *Reader) readSlice(path string, pipeIn *io.PipeWriter) (err error) {
	// Defer: close all readers in the chain, after the slice is processed
	var closers closer.Closers
	defer func() {
		if closeErr := closers.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			}
		}
	}()

	// Open the file
	var sliceReader io.Reader
	if file, err := os.OpenFile(path, os.O_RDONLY, 0); err == nil {
		sliceReader = file
		closers.Append(func() error {
			return file.Close()
		})
	} else {
		return err
	}

	// Add decompression
	if strings.HasSuffix(path, kbc.GzipFileExtension) {
		if gzipReader, err := r.gzipReaders.ReaderFrom(sliceReader); err == nil {
			sliceReader = gzipReader
			closers.Append(func() error {
				return gzipReader.Close()
			})
		} else {
			return err
		}
	}

	// Stream data to the pipe
	_, err = io.Copy(pipeIn, sliceReader)
	return err
}
