package slicedWriter

import (
	"fmt"
	"keboola.processor-split-table/src/config"
)

// SlicedWriter writes CSV to a sliced table defined by dirPath.
// Each part is one file in dirPath.
// When maxRows/maxBytes is reached -> a new file/part is created.
type SlicedWriter struct {
	conf        *config.Config
	dirPath     string
	sliceNumber uint32
	slice       *slice
	allRows     uint64
	allBytes    uint64
}

func NewSlicedWriter(conf *config.Config, dirPath string) *SlicedWriter {
	w := &SlicedWriter{conf: conf, dirPath: dirPath}
	w.createNextSlice() // open first slice
	return w
}

func (w *SlicedWriter) Write(row []byte) {
	rowLength := uint64(len(row))
	if !w.slice.IsSpaceForNextRow(rowLength) {
		w.createNextSlice()
	}

	w.slice.Write(row, rowLength)
	w.allRows++
	w.allBytes += rowLength
}

func (w *SlicedWriter) Close() {
	w.slice.Close()
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

func (w *SlicedWriter) createNextSlice() {
	if w.slice != nil {
		w.slice.Close()
	}
	w.sliceNumber++
	w.slice = NewSlice(w.conf, w.dirPath+"/part"+fmt.Sprintf("%04d", w.sliceNumber))
}
