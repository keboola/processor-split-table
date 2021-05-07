package slicedWriter

import (
	"bufio"
	"fmt"
	"keboola.processor-split-by-nrows/src/config"
	"keboola.processor-split-by-nrows/src/kbc"
	"keboola.processor-split-by-nrows/src/utils"
	"os"
)

const OutBufferSize = 20 * 1024 * 1024 // 20MB

// SlicedWriter writes CSV to a sliced table defined by dirPath.
// Each part is one file in dirPath.
// When rowsPerSlice/bytesPerSlice is reached -> a new file/part is created.
type SlicedWriter struct {
	mode          config.Mode
	bytesPerSlice uint64
	rowsPerSlice  uint64
	dirPath       string
	slice         uint32 // 1,2,3 ...
	slicePath     string
	sliceFile     *os.File
	sliceWriter   *bufio.Writer
	sliceRows     uint64
	sliceBytes    uint64
	allRows       uint64
	allBytes      uint64
}

func NewSlicedWriter(conf *config.Config, dirPath string) *SlicedWriter {
	w := &SlicedWriter{
		conf.Parameters.Mode,
		conf.Parameters.BytesPerSlice,
		conf.Parameters.RowsPerSlice,
		dirPath,
		0,
		"",
		nil,
		nil,
		0,
		0,
		0,
		0,
	}
	w.createNextSlice() // open first part
	return w
}

func (w *SlicedWriter) Write(row []byte) {
	rowLength := uint64(len(row))
	if !w.isSpaceForNextRow(rowLength) {
		w.createNextSlice()
	}

	_, err := w.sliceWriter.Write(row)
	if err != nil {
		kbc.PanicApplicationError("Cannot write row to file \"%s\": %s", w.slicePath, err)
	}

	w.sliceRows++
	w.allRows++
	w.sliceBytes += rowLength
	w.allBytes += rowLength
}

func (w *SlicedWriter) Close() {
	utils.FlushWriter(w.sliceWriter, w.slicePath)
	utils.CloseFile(w.sliceFile, w.slicePath)
}

func (w *SlicedWriter) Slices() uint32 {
	return w.slice
}

func (w *SlicedWriter) AllRows() uint64 {
	return w.allRows
}

func (w *SlicedWriter) AlLBytes() uint64 {
	return w.allBytes
}

func (w *SlicedWriter) createNextSlice() {
	utils.FlushWriter(w.sliceWriter, w.slicePath)
	utils.CloseFile(w.sliceFile, w.slicePath)
	w.slice++
	w.slicePath = w.dirPath + "/part" + fmt.Sprintf("%04d", w.slice)
	w.sliceFile = utils.OpenFile(w.slicePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	w.sliceWriter = bufio.NewWriterSize(w.sliceFile, OutBufferSize)
	w.sliceRows = 0
	w.sliceBytes = 0
}

func (w *SlicedWriter) isSpaceForNextRow(rowLength uint64) bool {
	switch w.mode {
	case config.ModeBytes:
		return w.sliceBytes+rowLength <= w.bytesPerSlice
	case config.ModeRows:
		return w.sliceRows < w.rowsPerSlice
	default:
		kbc.PanicApplicationError("Unexpected sliced writer mode \"%s\".", w.mode)
		return false // unreachable
	}
}
