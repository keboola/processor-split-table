package rowsreader

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/pgzip"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/columnsparser"
)

const (
	StartTokenBufferSize = 512 * 1024       // 512kB, initial size of buffer, it is auto-scaled
	MaxTokenBufferSize   = 50 * 1024 * 1024 // 50MB, max size of buffer -> max size of one row
)

// CSVReader reads rows from the CSV table.
// When slicing, we do not need to decode the individual columns, we just need to reliably determine the rows.
// Therefore, this own/fast implementation.
type CSVReader struct {
	closers       []io.Closer
	slicedInput   bool
	path          string
	rowNumber     uint64
	slicesCounter *uint32
	scanner       *bufio.Scanner
	delimiter     byte
	enclosure     byte
}

// NewSlicesReader creates the CSVReader for a sliced CSV table.
func NewSlicesReader(dirPath string, delimiter byte, enclosure byte) (*CSVReader, error) {
	// Count input slices in the WalkDir
	var slicesCounter uint32

	// Stream slices to the pipe
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		// Iterate all slices
		err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, walkErr error) (err error) {
			// Stop on error
			if walkErr != nil {
				return err
			}

			// Skip top dir
			if path == dirPath {
				return nil
			}

			// Handle unexpected subdirectory
			if d.IsDir() {
				return fmt.Errorf(`unexpected directory "%s"`, path)
			}

			// Defer: close all readers in the chain, after the slice is processed
			var closers []io.Closer
			defer func() {
				for i := len(closers) - 1; i >= 0; i-- {
					if closeErr := closers[i].Close(); closeErr != nil {
						if err == nil {
							err = closeErr
						}
					}
				}
			}()

			// Open the slice
			slicesCounter++
			sliceReader, newClosers, err := openCSVSlice(path)
			if err == nil {
				closers = append(closers, newClosers...)
			} else {
				return err
			}

			// Stream data to the pipe
			if _, err = io.Copy(pipeWriter, sliceReader); err != nil {
				return err
			}

			// Continue to the next slice, if any
			return nil
		})

		// Send WalkDir error to the reading side
		if err == nil {
			_ = pipeWriter.Close()
		} else {
			_ = pipeWriter.CloseWithError(fmt.Errorf(`error when iterating slices in "%s": %w`, dirPath, err))
		}
	}()

	return newCSVReader(pipeReader, []io.Closer{pipeReader}, &slicesCounter, true, dirPath, delimiter, enclosure)
}

// NewFileReader creates the CSVReader for a single CSV file.
func NewFileReader(filePath string, delimiter byte, enclosure byte) (*CSVReader, error) {
	slicesCounter := uint32(1)
	if reader, closers, err := openCSVSlice(filePath); err == nil {
		return newCSVReader(reader, closers, &slicesCounter, false, filePath, delimiter, enclosure)
	} else {
		return nil, err
	}
}

// openCSVSlice opens a CSV slice.
func openCSVSlice(filePath string) (reader io.Reader, closers []io.Closer, err error) {
	var readCloser io.ReadCloser

	// Open the file
	readCloser, err = os.OpenFile(filePath, os.O_RDONLY, 0)
	if err == nil {
		closers = append(closers, readCloser)
	} else {
		return nil, closers, err
	}

	// Decompress the file
	if strings.HasSuffix(filePath, kbc.GzipFileExtension) {
		readCloser, err = pgzip.NewReader(readCloser)
		if err == nil {
			closers = append(closers, readCloser)
		} else {
			return nil, closers, err
		}
	}

	return readCloser, closers, nil
}

func newCSVReader(reader io.Reader, closers []io.Closer, slicesCounter *uint32, slicedInput bool, path string, delimiter byte, enclosure byte) (*CSVReader, error) {
	// Create scanner with custom split function
	buffer := make([]byte, StartTokenBufferSize)
	scanner := bufio.NewScanner(reader)
	scanner.Split(getSplitRowsFunc(enclosure))
	scanner.Buffer(buffer, MaxTokenBufferSize)
	return &CSVReader{
		closers:       closers,
		slicedInput:   slicedInput,
		path:          path,
		slicesCounter: slicesCounter,
		scanner:       scanner,
		delimiter:     delimiter,
		enclosure:     enclosure,
	}, nil
}

func (r *CSVReader) Slices() uint32 {
	return *r.slicesCounter
}

func (r *CSVReader) Header() ([]string, error) {
	// The method can be used only with non-sliced input
	if r.slicedInput {
		return nil, fmt.Errorf(
			`the header cannot be read from the sliced file "%s", the header should be present in the manifest`,
			filepath.Base(r.path),
		)
	}

	// Header can only be read if no row has been read yet
	if r.rowNumber != 0 {
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
	header := r.Bytes()
	p := columnsparser.NewParser(r.delimiter, r.enclosure)
	columns, err := p.Parse(header)
	if err != nil {
		return nil, fmt.Errorf("cannot parse CSV header: %w", err)
	}

	return columns, nil
}

func (r *CSVReader) Read() bool {
	ok := r.scanner.Scan()
	if ok {
		r.rowNumber++
	}

	return ok
}

func (r *CSVReader) Bytes() []byte {
	return r.scanner.Bytes()
}

func (r *CSVReader) Close() (err error) {
	for i := len(r.closers) - 1; i >= 0; i-- {
		if closeErr := r.closers[i].Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			}
		}
	}

	if scannerErr := r.scanner.Err(); scannerErr != nil {
		err = scannerErr
	}

	return err
}
