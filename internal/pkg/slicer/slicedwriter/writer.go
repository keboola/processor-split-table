package slicedwriter

import (
	"fmt"
	"math"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/processor-split-table/internal/pkg/pool"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

// Writer writes CSV to a sliced table directory.
// Each part is one file in the directory.
// When maxRows/maxBytes is reached -> a new file/slice is created.
type Writer struct {
	config        config.Config
	bufferWriters *pool.BufferWriterPool
	gzipWriters   *pool.GZIPWriterPool
	outPath       string
	sliceNumber   uint32
	slice         *slice
	allRows       uint64
	allBytes      datasize.ByteSize
}

func New(cfg config.Config, totalInputSize datasize.ByteSize, outPath string) (*Writer, error) {
	// Convert NumberOfSlices to BytesPerSlice
	if cfg.Mode == config.ModeSlices {
		cfg.Mode = config.ModeBytes
		cfg.BytesPerSlice = datasize.ByteSize(math.Ceil(float64(totalInputSize) / float64(cfg.NumberOfSlices)))

		// Too small slices (a few kilobytes) can slow down upload -> check min size
		if cfg.BytesPerSlice < cfg.MinBytesPerSlice {
			cfg.BytesPerSlice = cfg.MinBytesPerSlice
		}
	} else {
		cfg.NumberOfSlices = 0 // disabled
	}

	w := &Writer{
		config:        cfg,
		bufferWriters: pool.BufferedWriters(cfg.BufferSize),
		gzipWriters:   pool.GZIPWriters(cfg.GzipLevel, cfg.GzipBlockSize, int(cfg.GzipConcurrency)),
		outPath:       outPath,
	}

	// Open first slice
	if err := w.createNextSlice(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Writer) Write(row []byte) error {
	rowLength := uint64(len(row))
	if !w.IsSpaceForNextRowInSlice(rowLength) {
		if err := w.createNextSlice(); err != nil {
			return err
		}
	}

	if err := w.slice.Write(row, rowLength); err != nil {
		return err
	}

	w.allRows++
	w.allBytes += datasize.ByteSize(rowLength)
	return nil
}

func (w *Writer) Close() error {
	return w.slice.Close()
}

func (w *Writer) IsSpaceForNextRowInSlice(rowLength uint64) bool {
	// Last slice, do not overflow
	if w.config.NumberOfSlices > 0 && w.config.NumberOfSlices == w.sliceNumber {
		return true
	}

	return w.slice.IsSpaceForNextRow(rowLength)
}

func (w *Writer) GzipEnabled() bool {
	return w.config.Gzip
}

func (w *Writer) Slices() uint32 {
	return w.sliceNumber
}

func (w *Writer) AllRows() uint64 {
	return w.allRows
}

func (w *Writer) AlLBytes() datasize.ByteSize {
	return w.allBytes
}

func (w *Writer) createNextSlice() error {
	if w.slice != nil {
		if err := w.slice.Close(); err != nil {
			return err
		}
	}

	w.sliceNumber++
	path := getSlicePath(w.outPath, w.sliceNumber, w.config.Gzip)

	s, err := w.newSlice(path)
	if err != nil {
		return err
	}

	w.slice = s
	return nil
}

func getSlicePath(dirPath string, sliceNumber uint32, gzip bool) string {
	path := dirPath + "/part" + fmt.Sprintf("%04d", sliceNumber)
	if gzip {
		path += ".gz"
	}
	return path
}
