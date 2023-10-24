package rowsreader

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
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

	csvReader, err := NewFileReader(filepath.Join(rootDir, "fixtures", "two_rows.csv"), ',', '"')
	require.NoError(t, err)

	header, err := csvReader.Header()
	require.NoError(t, err)
	assert.Equal(t, []string{"abc", "def"}, header)
}

func TestReadHeaderCannotParse(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	csvReader, err := NewFileReader(filepath.Join(rootDir, "fixtures", "bad_header.csv"), ',', '"')
	require.NoError(t, err)

	_, err = csvReader.Header()
	if assert.Error(t, err) {
		assert.Equal(t, "cannot parse CSV header: unexpected token \"missing enclosure\n1\" before enclosure at position 28", err.Error())
	}
}

func TestReadHeaderRowAlreadyRead(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	csvReader, err := NewFileReader(filepath.Join(rootDir, "fixtures", "two_rows.csv"), ',', '"')
	require.NoError(t, err)

	csvReader.Read()

	_, err = csvReader.Header()
	if assert.Error(t, err) {
		assert.Equal(t, `the header cannot be read, other lines have already been read from CSV "two_rows.csv"`, err.Error())
	}
}

func TestReadHeaderEmptyFile(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	csvReader, err := NewFileReader(filepath.Join(rootDir, "fixtures", "empty.csv"), ',', '"')
	require.NoError(t, err)

	_, err = csvReader.Header()
	if assert.Error(t, err) {
		assert.Equal(t, `missing header row in CSV "empty.csv"`, err.Error())
	}
}

func TestReadHeaderSlicedFile(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	path := filepath.Join(rootDir, "fixtures", "sliced.csv")
	slices, err := kbc.FindSlices(path)
	require.NoError(t, err)

	csvReader, err := NewSlicesReader(path, slices, ',', '"')
	require.NoError(t, err)

	_, err = csvReader.Header()
	if assert.Error(t, err) {
		assert.Equal(t, `the header cannot be read from the sliced file "sliced.csv", the header should be present in the manifest`, err.Error())
	}
}

func TestReadCSVFile(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	for _, testData := range getReadCsvTestData() {
		var rows []string

		csvReader, err := NewFileReader(filepath.Join(rootDir, "fixtures", testData.csvPath), ',', '"')
		require.NoError(t, err)

		for csvReader.Read() {
			rows = append(rows, string(csvReader.Bytes()))
		}
		assert.Equal(t, testData.expectedErr, csvReader.Close(), testData.csvPath)
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
