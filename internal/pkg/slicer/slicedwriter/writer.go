package slicedwriter

import (
	"fmt"
	"math"

	"github.com/keboola/processor-split-table/internal/pkg/processor/config"
)

// SlicedWriter writes CSV to a sliced table defined by dirPath.
// Each part is one file in dirPath.
// When maxRows/maxBytes is reached -> a new file/part is created.
type SlicedWriter struct {
	mode          config.Mode
	bytesPerSlice uint64
	rowsPerSlice  uint64
	maxSlices     uint32
	gzipEnabled   bool
	gzipLevel     int
	dirPath       string
	sliceNumber   uint32
	slice         *slice
	allRows       uint64
	allBytes      uint64
}

func NewSlicedWriterFromConf(conf *config.Config, inFileSize uint64, outPath string) (*SlicedWriter, error) {
	mode := conf.Parameters.Mode
	bytesPerSlice := conf.Parameters.BytesPerSlice
	rowsPerSlice := conf.Parameters.RowsPerSlice
	maxSlices := conf.Parameters.NumberOfSlices

	// Fixed number of slices -> calculate bytesPerSlice
	if mode == config.ModeSlices {
		mode = config.ModeBytes
		fileSize := float64(inFileSize)
		bytesPerSlice = uint64(math.Ceil(fileSize / float64(maxSlices)))

		// Too small slices (a few kilobytes) can slow down upload -> check min size
		if bytesPerSlice < conf.Parameters.MinBytesPerSlice {
			bytesPerSlice = conf.Parameters.MinBytesPerSlice
		}
	} else {
		maxSlices = 0 // disabled
	}

	return NewSlicedWriter(mode, bytesPerSlice, rowsPerSlice, maxSlices, conf.Parameters.Gzip, conf.Parameters.GzipLevel, outPath)
}

func NewSlicedWriter(mode config.Mode, bytesPerSlice uint64, rowsPerSlice uint64, maxSlices uint32, gzipEnabled bool, gzipLevel int, dirPath string) (*SlicedWriter, error) {
	w := &SlicedWriter{
		mode,
		bytesPerSlice,
		rowsPerSlice,
		maxSlices,
		gzipEnabled,
		gzipLevel,
		dirPath,
		0,
		nil,
		0,
		0,
	}

	// Open first slice
	if err := w.createNextSlice(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *SlicedWriter) Write(row []byte) error {
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
	w.allBytes += rowLength
	return nil
}

func (w *SlicedWriter) Close() error {
	return w.slice.Close()
}

func (w *SlicedWriter) IsSpaceForNextRowInSlice(rowLength uint64) bool {
	// Last slice, do not overflow
	if w.maxSlices > 0 && w.maxSlices == w.sliceNumber {
		return true
	}

	return w.slice.IsSpaceForNextRow(rowLength)
}

func (w *SlicedWriter) GzipEnabled() bool {
	return w.gzipEnabled
}

func (w *SlicedWriter) Slices() uint32 {
	return w.sliceNumber
}

func (w *SlicedWriter) AllRows() uint64 {
	return w.allRows
}

func (w *SlicedWriter) AlLBytes() uint64 {
	return w.allBytes
}

func (w *SlicedWriter) createNextSlice() error {
	if w.slice != nil {
		if err := w.slice.Close(); err != nil {
			return err
		}
	}

	w.sliceNumber++
	path := getSlicePath(w.dirPath, w.sliceNumber, w.gzipEnabled)

	s, err := newSlice(w.mode, w.bytesPerSlice, w.rowsPerSlice, w.gzipEnabled, w.gzipLevel, path)
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
