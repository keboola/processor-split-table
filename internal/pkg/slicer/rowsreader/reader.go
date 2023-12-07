package rowsreader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/readahead"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/pool"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/closer"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/columnsparser"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/rowsreader/progress"
)

const (
	// StartTokenBufferSize specifies initial size of the scanner buffer.
	StartTokenBufferSize = int(8 * datasize.MB)
	// MaxTokenBufferSize specifies maximal size of the scanner buffer, it is also maximum length of a CSV row.
	MaxTokenBufferSize = int(50 * datasize.MB)
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

	progress     *progress.Logger
	closers      closer.Closers
	scanner      *bufio.Scanner
	gzipReaders  *pool.GZIPReaderPool
	aheadBuffers *pool.ReadAheadBuffersPool
}

type sliceReadCloser struct {
	io.Reader
	closer.Closers
}

// NewSlicesReader creates the Reader for a sliced CSV table.
func NewSlicesReader(progress *progress.Logger, cfg config.Config, path string, slices kbc.Slices, delimiter byte, enclosure byte) (*Reader, error) {
	return newReader(progress, cfg, path, slices.Paths(), true, delimiter, enclosure)
}

// NewFileReader creates the Reader for a single CSV file.
// It is special case of the slices reader with only one slice.
func NewFileReader(progress *progress.Logger, cfg config.Config, path string, delimiter byte, enclosure byte) (*Reader, error) {
	return newReader(progress, cfg, path, []string{path}, false, delimiter, enclosure)
}

func newReader(progress *progress.Logger, cfg config.Config, path string, slices []string, sliced bool, delimiter byte, enclosure byte) (*Reader, error) {
	reader := &Reader{
		progress:     progress,
		config:       cfg,
		path:         path,
		slices:       slices,
		delimiter:    delimiter,
		enclosure:    enclosure,
		sliced:       sliced,
		gzipReaders:  pool.GZIPReaders(),
		aheadBuffers: pool.ReadAheadBuffers(int(cfg.AheadBlocks), cfg.AheadBlockSize),
	}

	// Create pipe to merge content of the slices
	pipeOut, pipeIn := io.Pipe()
	reader.closers.Append(pipeOut.Close)

	// Stream slices to the pipe
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
	if err := r.closers.Close(); err != nil {
		return err
	}

	if err := r.scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (r *Reader) readSlicesTo(pipeIn *io.PipeWriter) {
	// The readers channel is buffered.
	// The specified number of slices will be opened ahead and the beginning of the slice will be preloaded.
	// This ensures a smooth transition between slices, without losing performance.
	readers := make(chan *sliceReadCloser, r.config.AheadSlices)
	grp, ctx := errgroup.WithContext(context.Background())

	// Open multiple readers in background
	grp.Go(func() error {
		defer close(readers)
		for _, path := range r.slices {
			// Create slice read ahead reader
			sliceReader, err := r.openSlice(path)
			if err != nil {
				return err
			}

			// Send reader to the buffered channel
			select {
			case <-ctx.Done():
				return nil
			case readers <- sliceReader:
				// continue
			}
		}
		return nil
	})

	// Copy data from slice readers to the pipe
	grp.Go(func() error {
		for sliceReader := range readers {
			select {
			case <-ctx.Done():
				return nil
			default:
				_, readErr := io.Copy(pipeIn, sliceReader)
				closeErr := sliceReader.Close()
				if readErr != nil {
					return readErr
				} else if closeErr != nil {
					return closeErr
				}
			}
		}
		return nil
	})

	// Note: error is processed on the reading side
	err := grp.Wait()
	_ = pipeIn.CloseWithError(err)
}

func (r *Reader) openSlice(path string) (*sliceReadCloser, error) {
	out := &sliceReadCloser{}

	// Open the file
	if file, err := os.OpenFile(path, os.O_RDONLY, 0); err == nil {
		out.Reader = file
		out.Closers.Append(file.Close)
	} else {
		return nil, err
	}

	// Measure the reading progress of each slice
	out.Reader = r.progress.NewMeter(out.Reader)

	// Add decompression
	if strings.HasSuffix(path, kbc.GzipFileExtension) {
		if gzipReader, err := r.gzipReaders.ReaderFrom(out.Reader); err == nil {
			out.Reader = gzipReader
			out.Closers.
				Append(func() error {
					defer r.gzipReaders.Put(gzipReader)
					return gzipReader.Close()
				})
		} else {
			return nil, err
		}
	}

	// Add read ahead buffer
	if r.config.AheadBlocks != 0 {
		buffers := r.aheadBuffers.Get()
		if aheadReader, err := readahead.NewReaderBuffer(out.Reader, *buffers); err == nil {
			out.Reader = aheadReader
			out.Closers.
				Append(func() error {
					defer r.aheadBuffers.Put(buffers)
					return aheadReader.Close()
				})
		} else {
			return nil, err
		}
	}

	return out, nil
}
