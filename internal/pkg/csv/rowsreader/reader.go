package rowsreader

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/keboola/processor-split-table/internal/pkg/csv/columnsparser"
	"github.com/keboola/processor-split-table/internal/pkg/kbc"
)

const (
	StartTokenBufferSize = 512 * 1024       // 512kB, initial size of buffer, it is auto-scaled
	MaxTokenBufferSize   = 50 * 1024 * 1024 // 50MB, max size of buffer -> max size of one row
)

// CsvReader reads rows from the CSV table.
// When slicing, we do not need to decode the individual columns, we just need to reliably determine the rows.
// Therefore, this own/fast implementation.
type CsvReader struct {
	path      string
	rowNumber uint64
	scanner   *bufio.Scanner
	delimiter byte
	enclosure byte
}

func NewCsvReader(csvPath string, delimiter byte, enclosure byte) (*CsvReader, error) {
	// Open CSV file
	file, err := os.OpenFile(csvPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	// Create scanner with custom split function
	buffer := make([]byte, StartTokenBufferSize)
	scanner := bufio.NewScanner(file)
	scanner.Split(getSplitRowsFunc(enclosure))
	scanner.Buffer(buffer, MaxTokenBufferSize)
	return &CsvReader{csvPath, 0, scanner, delimiter, enclosure}, nil
}

func (r *CsvReader) Header() ([]string, error) {
	// Header can only be read if no row has been read yet
	if r.rowNumber != 0 {
		return nil, fmt.Errorf(
			"the header cannot be read, other lines have already been read from CSV \"%s\"",
			filepath.Base(r.path),
		)
	}

	// Header must be present for tables that don't have columns in manifest.json
	if !r.Read() {
		return nil, kbc.UserErrorf("missing header row in CSV \"%s\"", filepath.Base(r.path))
	}

	// Check if no error
	if r.Err() != nil {
		return nil, kbc.UserErrorf("error when reading CSV header: %w", r.Err())
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

func getSplitRowsFunc(enclosure byte) bufio.SplitFunc {
	// Search for \n -> rows delimiter. \n between enclosures is ignored.
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		length := len(data)

		// Iterate over characters
		insideEnclosure := false
		for index, char := range data {
			switch char {
			case '\n':
				if !insideEnclosure {
					// Line break outside enclosure -> row delimiter, return row
					return index + 1, data[0 : index+1], nil
				}
			case enclosure:
				// Enclosure found, invert state
				insideEnclosure = !insideEnclosure
			}
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
}
