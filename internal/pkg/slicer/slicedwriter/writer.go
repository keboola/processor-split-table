package slicedwriter

import (
	"fmt"
	"math"

	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

// SlicedWriter writes CSV to a sliced table defined by outPath.
// Each part is one file in outPath.
// When maxRows/maxBytes is reached -> a new file/part is created.
type SlicedWriter struct {
	config      config.Config
	outPath     string
	sliceNumber uint32
	slice       *slice
	allRows     uint64
	allBytes    uint64
}

func New(cfg config.Config, inFileSize uint64, outPath string) (*SlicedWriter, error) {
	// Convert NumberOfSlices to BytesPerSlice
	if cfg.Mode == config.ModeSlices {
		cfg.Mode = config.ModeBytes
		fileSize := float64(inFileSize)
		cfg.BytesPerSlice = uint64(math.Ceil(fileSize / float64(cfg.NumberOfSlices)))

		// Too small slices (a few kilobytes) can slow down upload -> check min size
		if cfg.BytesPerSlice < cfg.MinBytesPerSlice {
			cfg.BytesPerSlice = cfg.MinBytesPerSlice
		}
	} else {
		cfg.NumberOfSlices = 0 // disabled
	}

	w := &SlicedWriter{config: cfg, outPath: outPath}

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
	if w.config.NumberOfSlices > 0 && w.config.NumberOfSlices == w.sliceNumber {
		return true
	}

	return w.slice.IsSpaceForNextRow(rowLength)
}

func (w *SlicedWriter) GzipEnabled() bool {
	return w.config.Gzip
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
	path := getSlicePath(w.outPath, w.sliceNumber, w.config.Gzip)

	s, err := newSlice(w.config, path)
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
