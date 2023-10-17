package rowsreader

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testDataForFunc struct {
	comment         string
	data            []byte
	atEOF           bool
	expectedAdvance int
	expectedToken   []byte
	expectedErr     error
}

type testDataForRead struct {
	csvPath      string
	expectedErr  error
	expectedRows []string
}

func TestReadHeader(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	csvReader := NewCsvReader(rootDir+"/fixtures/two_rows.csv", ',', '"')
	assert.Equal(t, []string{"abc", "def"}, csvReader.Header())
}

func TestReadHeaderCannotParse(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	csvReader := NewCsvReader(rootDir+"/fixtures/bad_header.csv", ',', '"')
	assert.PanicsWithError(t, "Cannot parse CSV header: unexpected token \"missing enclosure\n1\" before enclosure at position 28.", func() {
		csvReader.Header()
	})
}

func TestReadHeaderRowAlreadyRead(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	csvReader := NewCsvReader(rootDir+"/fixtures/two_rows.csv", ',', '"')
	csvReader.Read()
	assert.PanicsWithError(
		t,
		"The header cannot be read, other lines have already been read from CSV \"two_rows.csv\".",
		func() {
			csvReader.Header()
		},
	)
}

func TestReadHeaderEmptyFile(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	csvReader := NewCsvReader(rootDir+"/fixtures/empty.csv", ',', '"')
	assert.PanicsWithError(
		t,
		"Missing header row in CSV \"empty.csv\".",
		func() {
			csvReader.Header()
		},
	)
}

func TestReadCsv(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	for _, testData := range getReadCsvTestData() {
		var rows []string
		csvReader := NewCsvReader(rootDir+"/fixtures/"+testData.csvPath, ',', '"')
		for csvReader.Read() {
			rows = append(rows, string(csvReader.Bytes()))
		}
		assert.Equal(t, testData.expectedErr, csvReader.Err(), testData.csvPath)
		assert.Equal(t, testData.expectedRows, rows, testData.csvPath)
	}
}

// Test for splitting function.
func TestSplitRowsFunc(t *testing.T) {
	t.Parallel()

	splitRowsFunc := getSplitRowsFunc('"')
	for _, testData := range getSplitRowsFuncTestData() {
		advance, token, err := splitRowsFunc(testData.data, testData.atEOF)
		assert.Equal(t, testData.expectedAdvance, advance, testData.comment)
		assert.Equal(t, testData.expectedToken, token, testData.comment)
		assert.Equal(t, testData.expectedErr, err, testData.comment)
	}
}

func getSplitRowsFuncTestData() []testDataForFunc {
	return []testDataForFunc{
		{
			comment:         "Empty data -> no token",
			data:            []byte(""),
			atEOF:           false,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "Empty data at the end -> no token",
			data:            []byte(""),
			atEOF:           true,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "One row",
			data:            []byte("abc,def\n"),
			atEOF:           false,
			expectedAdvance: 8,
			expectedToken:   []byte("abc,def\n"),
			expectedErr:     nil,
		},
		{
			comment:         "One row at the end",
			data:            []byte("abc,def\n"),
			atEOF:           true,
			expectedAdvance: 8,
			expectedToken:   []byte("abc,def\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Two rows -> first row parsed",
			data:            []byte("abc,def\nfgh,xyz\n"),
			atEOF:           false,
			expectedAdvance: 8,
			expectedToken:   []byte("abc,def\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Two rows at the end -> first row parsed",
			data:            []byte("abc,def\nfgh,xyz\n"),
			atEOF:           true,
			expectedAdvance: 8,
			expectedToken:   []byte("abc,def\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Incomplete row -> load more data",
			data:            []byte("abc,def"),
			atEOF:           false,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "Incomplete row with enclosure -> load more data",
			data:            []byte("\"abc\",\"def\""),
			atEOF:           false,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "Incomplete row at the end -> ok, last row without new line",
			data:            []byte("abc,def"),
			atEOF:           true,
			expectedAdvance: 7,
			expectedToken:   []byte("abc,def"),
			expectedErr:     nil,
		},
		{
			comment:         "Row with enclosures 1",
			data:            []byte("\"abc\"\n"),
			atEOF:           false,
			expectedAdvance: 6,
			expectedToken:   []byte("\"abc\"\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Row with enclosures 2",
			data:            []byte("\"abc\",def,\"xyz\"\n"),
			atEOF:           false,
			expectedAdvance: 16,
			expectedToken:   []byte("\"abc\",def,\"xyz\"\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Row with enclosures 3",
			data:            []byte("\"abc\",\"def\",\"xyz\"\n"),
			atEOF:           false,
			expectedAdvance: 18,
			expectedToken:   []byte("\"abc\",\"def\",\"xyz\"\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Unfinished enclosure 1 -> load more data",
			data:            []byte("\""),
			atEOF:           false,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "Unfinished enclosure 2 -> load more data",
			data:            []byte("\"abc\",\"def"),
			atEOF:           false,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "Unfinished enclosure at the end -> return last row",
			data:            []byte("\"abc\",\"def"),
			atEOF:           true,
			expectedAdvance: 10,
			expectedToken:   []byte("\"abc\",\"def"),
			expectedErr:     nil,
		},
		{
			comment:         "Unfinished enclosure with new line 1 -> load more data",
			data:            []byte("\"\n"),
			atEOF:           false,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "Unfinished enclosure with new line 2 -> load more data",
			data:            []byte("\"abc\n\",\"def\n"),
			atEOF:           false,
			expectedAdvance: 0,
			expectedToken:   nil,
			expectedErr:     nil,
		},
		{
			comment:         "Unfinished enclosure with new line at the end -> return last row",
			data:            []byte("\"abc\n\",\"def\n"),
			atEOF:           true,
			expectedAdvance: 12,
			expectedToken:   []byte("\"abc\n\",\"def\n"),
			expectedErr:     nil,
		},
		{
			comment:         "One row with escaped new line",
			data:            []byte("\"abc\nxyz\",\"\ndef\n\"\n"),
			atEOF:           false,
			expectedAdvance: 18,
			expectedToken:   []byte("\"abc\nxyz\",\"\ndef\n\"\n"),
			expectedErr:     nil,
		},
		{
			comment:         "One row at the end with escaped new line",
			data:            []byte("\"abc\nxyz\",\"\ndef\n\"\n"),
			atEOF:           true,
			expectedAdvance: 18,
			expectedToken:   []byte("\"abc\nxyz\",\"\ndef\n\"\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Two rows with escaped new line",
			data:            []byte("\"abc\nxyz\",\"\ndef\n\"\n\"123\n\",\"456\n\""),
			atEOF:           false,
			expectedAdvance: 18,
			expectedToken:   []byte("\"abc\nxyz\",\"\ndef\n\"\n"),
			expectedErr:     nil,
		},
		{
			comment:         "Two rows at the end with escaped new line",
			data:            []byte("\"abc\nxyz\",\"\ndef\n\"\n\"123\n\",\"456\n\""),
			atEOF:           true,
			expectedAdvance: 18,
			expectedToken:   []byte("\"abc\nxyz\",\"\ndef\n\"\n"),
			expectedErr:     nil,
		},
	}
}

func getReadCsvTestData() []testDataForRead {
	return []testDataForRead{
		{
			csvPath:      "empty.csv",
			expectedErr:  nil,
			expectedRows: nil,
		},
		{
			csvPath:     "empty_with_new_line.csv",
			expectedErr: nil,
			expectedRows: []string{
				"\n",
			},
		},
		{
			csvPath:     "one_row.csv",
			expectedErr: nil,
			expectedRows: []string{
				"\"abc\",\"def\"\n",
			},
		},
		{
			csvPath:     "two_rows.csv",
			expectedErr: nil,
			expectedRows: []string{
				"\"abc\",\"def\"\n",
				"\"123\",\"456\"\n",
			},
		},
		{
			csvPath:     "escaping.csv",
			expectedErr: nil,
			expectedRows: []string{
				"\"col1\",\"col2\"\n",
				"\"line with enclosure\",\"second column\"\n",
				"\"column with enclosure \"\"\"\", and comma inside text\",\"second column enclosure in text \"\"\"\"\"\n",
				"\"columns with\n                new line\",\"columns with \ttab\"\n",
				"\"column with backslash \\ inside\",\"column with backslash and enclosure \\\"\"\\\"\"\"\n",
				"\"column with \\n \\t \\\",\"second col\"\n",
				"\"unicode characters\",\"ľščťžýáíéúäôň\"\n",
				"\"first\",\"something with\n\n                double new line\"\n",
			},
		},
	}
}
