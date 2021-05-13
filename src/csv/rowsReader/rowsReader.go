package rowsReader

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/utils"
	"os"
	"path/filepath"
)

const (
	StartTokenBufferSize      = 512 * 1024       // 512kB, initial size of buffer, it is auto-scaled
	MaxTokenBufferSize        = 50 * 1024 * 1024 // 50MB, max size of buffer -> max size of one row
	CsvLineBreak         byte = '\n'
	CsvEnclosure         byte = '"' // used double-enclosure escaping in code
)

// CsvReader reads rows from the CSV table.
// When slicing, we do not need to decode the individual columns, we just need to reliably determine the rows.
// Therefore, this own/fast implementation.
type CsvReader struct {
	path      string
	rowNumber uint64
	scanner   *bufio.Scanner
}

func NewCsvReader(csvPath string) *CsvReader {
	// Open CSV file
	file := utils.OpenFile(csvPath, os.O_RDONLY)

	// Create scanner with custom split function
	buffer := make([]byte, StartTokenBufferSize)
	scanner := bufio.NewScanner(file)
	scanner.Split(splitRowsFunc)
	scanner.Buffer(buffer, MaxTokenBufferSize)
	return &CsvReader{path: csvPath, rowNumber: 0, scanner: scanner}
}

func (r *CsvReader) Header() []string {
	// Header can only be read if no row has been read yet
	if r.rowNumber != 0 {
		kbc.PanicApplicationError(
			"The header cannot be read, other lines have already been read from CSV \"%s\".",
			filepath.Base(r.path),
		)
	}

	// Header must be present for tables that don't have columns in manifest.json
	if !r.Read() {
		kbc.PanicUserError("Missing header row in CSV \"%s\".", filepath.Base(r.path))
	}

	// We parse only whole rows in this processor,
	// ... but we only need individual columns for the header -> used Go CSV reader for this task
	recordsReader := csv.NewReader(bytes.NewReader(r.Bytes()))
	records, err := recordsReader.Read()
	if err != nil {
		kbc.PanicApplicationError("Cannot read header row in CSV \"%s\": %s", r.path, err)
	}

	return records
}

func (r *CsvReader) Read() bool {
	ok := r.scanner.Scan()
	if ok {
		r.rowNumber++
	}

	return ok
}

func (r *CsvReader) Bytes() []byte {
	return r.scanner.Bytes()
}

func (r *CsvReader) Err() error {
	return r.scanner.Err()
}

func splitRowsFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	length := len(data)

	// Iterate over each character
	index := 0
	insideEnclosure := false
	for index < length {
		switch data[index] {
		case CsvLineBreak:
			if !insideEnclosure {
				// Line break outside enclosure -> row delimiter, return row
				return index + 1, data[0 : index+1], nil
			}
		case CsvEnclosure:
			// We need to check next char, if 2 enclosures in a row -> then it is escaped enclosure -> skip
			nextIndex := index + 1
			nextAvailable := nextIndex < length

			// Request more data if needed (next char not loaded yet)
			if !atEOF && !nextAvailable {
				// Request more data, we don't have next char loaded, we cannot decide
				return 0, nil, nil
			}

			// Check next char
			if nextAvailable && data[nextIndex] == CsvEnclosure {
				// Escaped enclosure, skip next char
				index += 2
				continue
			}

			// Enclosure found, invert state
			insideEnclosure = !insideEnclosure
		}

		index++
	}

	// End of file
	if atEOF {
		if length == 0 {
			// All data consumed, no new token
			return 0, nil, nil
		}
		// The rest of the data is the last token/row
		return length, data, nil
	}

	// Request more data
	return 0, nil, nil
}
