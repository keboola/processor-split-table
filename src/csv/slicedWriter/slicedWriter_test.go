package slicedWriter

import (
	"github.com/stretchr/testify/assert"
	"keboola.processor-split-table/src/config"
	"keboola.processor-split-table/src/utils"
	"path/filepath"
	"runtime"
	"testing"
)

type testData struct {
	conf         *config.Config
	rows         []string
	expectedErr  error
	expectedPath string
}

func TestNewSlicedWriter(t *testing.T) {
	// Create temp dir
	tempDir := t.TempDir()

	// Config
	conf := &config.Config{
		Parameters: config.Parameters{
			Mode:          config.ModeBytes,
			BytesPerSlice: 123,
		},
	}

	// Create writer
	w := NewSlicedWriter(conf, tempDir)

	// Assert
	assert.Equal(t, config.ModeBytes, w.mode)
	assert.Equal(t, uint64(123), w.bytesPerSlice)
	assert.Equal(t, uint64(0), w.rowsPerSlice)
	assert.Equal(t, tempDir, w.dirPath)
	assert.Equal(t, uint32(1), w.slice)               // <<<<<<
	assert.Equal(t, tempDir+"/part0001", w.slicePath) // <<<<<<
	assert.NotNil(t, w.sliceFile)
	assert.NotNil(t, w.sliceWriter)
	assert.Equal(t, uint64(0), w.sliceRows)
	assert.Equal(t, uint64(0), w.sliceBytes)
	assert.Equal(t, uint64(0), w.allRows)
	assert.Equal(t, uint64(0), w.allBytes)
}

func TestCreateNextSlice(t *testing.T) {
	// Create temp dir
	tempDir := t.TempDir()

	// Config
	conf := &config.Config{
		Parameters: config.Parameters{
			Mode:          config.ModeBytes,
			BytesPerSlice: 123,
		},
	}

	// Create writer
	w := NewSlicedWriter(conf, tempDir)
	w.createNextSlice()

	// Assert
	assert.Equal(t, config.ModeBytes, w.mode)
	assert.Equal(t, uint64(123), w.bytesPerSlice)
	assert.Equal(t, uint64(0), w.rowsPerSlice)
	assert.Equal(t, tempDir, w.dirPath)
	assert.Equal(t, uint32(2), w.slice)               // <<<<<<
	assert.Equal(t, tempDir+"/part0002", w.slicePath) // <<<<<<
	assert.NotNil(t, w.sliceFile)
	assert.NotNil(t, w.sliceWriter)
	assert.Equal(t, uint64(0), w.sliceRows)
	assert.Equal(t, uint64(0), w.sliceBytes)
	assert.Equal(t, uint64(0), w.allRows)
	assert.Equal(t, uint64(0), w.allBytes)
}

func TestIsSpaceForNextRowBytes(t *testing.T) {
	// Create temp dir
	tempDir := t.TempDir()

	// Config
	conf := &config.Config{
		Parameters: config.Parameters{
			Mode:          config.ModeBytes,
			BytesPerSlice: 123, // <<<<<<
		},
	}

	// Create writer
	w := NewSlicedWriter(conf, tempDir)
	w.allRows = 10
	w.allBytes = 200
	w.sliceRows = 5
	w.sliceBytes = 100 // <<<<<< 23 bytes left

	// Assert
	assert.True(t, w.isSpaceForNextRow(22))
	assert.True(t, w.isSpaceForNextRow(23))
	assert.False(t, w.isSpaceForNextRow(24))
	assert.False(t, w.isSpaceForNextRow(25))
}

func TestIsSpaceForNextRowRows(t *testing.T) {
	// Create temp dir
	tempDir := t.TempDir()

	// Config
	conf := &config.Config{
		Parameters: config.Parameters{
			Mode:         config.ModeRows,
			RowsPerSlice: 10,
		},
	}

	// Create writer
	w := NewSlicedWriter(conf, tempDir)
	w.allRows = 10
	w.allBytes = 200
	w.sliceRows = 5 // <<<<<< 5 rows left
	w.sliceBytes = 100

	// Assert
	assert.True(t, w.isSpaceForNextRow(123))
	assert.True(t, w.isSpaceForNextRow(123))
	w.rowsPerSlice = 5 // <<<<<< no row left
	assert.False(t, w.isSpaceForNextRow(123))
	assert.False(t, w.isSpaceForNextRow(123))
}

func TestBytesMode(t *testing.T) {
	// Create temp dir
	tempDir := t.TempDir()

	// Config
	conf := &config.Config{
		Parameters: config.Parameters{
			Mode:          config.ModeBytes,
			BytesPerSlice: 40,
		},
	}

	// Create writer
	w := NewSlicedWriter(conf, tempDir)

	// 1 slice
	w.Write([]byte("\"1bc\",\"def\"\n")) // <<<<<< 12B
	assert.Equal(t, uint32(1), w.slice)
	w.Write([]byte("\"2bc\",\"def\"\n"))
	assert.Equal(t, uint32(1), w.slice) // <<<<<< 24B
	w.Write([]byte("\"3bc\",\"def\"\n"))
	assert.Equal(t, uint32(1), w.slice) // <<<<<< 32B
	// 2 slice
	w.Write([]byte("\"4bc\",\"def\"\n"))
	assert.Equal(t, uint32(2), w.slice) // <<<<<< 44B -> new slice -> 12B
	w.Write([]byte("\"5bc\",\"def\"\n"))
	assert.Equal(t, uint32(2), w.slice)
	w.Write([]byte("\"6bc\",\"def\"\n"))
	assert.Equal(t, uint32(2), w.slice)
	// 3 slice
	w.Write([]byte("\"7bc\",\"def\"\n"))
	assert.Equal(t, uint32(3), w.slice)
}

func TestRowsMode(t *testing.T) {
	// Create temp dir
	tempDir := t.TempDir()

	// Config
	conf := &config.Config{
		Parameters: config.Parameters{
			Mode:         config.ModeRows,
			RowsPerSlice: 3,
		},
	}

	// Create writer
	w := NewSlicedWriter(conf, tempDir)

	// 1 slice
	w.Write([]byte("\"1bc\",\"def\"\n"))
	assert.Equal(t, uint32(1), w.slice)
	w.Write([]byte("\"2bc\",\"def\"\n"))
	assert.Equal(t, uint32(1), w.slice)
	w.Write([]byte("\"3bc\",\"def\"\n"))
	assert.Equal(t, uint32(1), w.slice)
	// 2 slice
	w.Write([]byte("\"4bc\",\"def\"\n"))
	assert.Equal(t, uint32(2), w.slice)
	w.Write([]byte("\"5bc\",\"def\"\n"))
	assert.Equal(t, uint32(2), w.slice)
	w.Write([]byte("\"6bc\",\"def\"\n"))
	assert.Equal(t, uint32(2), w.slice)
	// 3 slice
	w.Write([]byte("\"7bc\",\"def\"\n"))
	assert.Equal(t, uint32(3), w.slice)
}

func TestWriteCsv(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	for _, testData := range getReadCsvTestData() {
		tempDir := t.TempDir()
		w := NewSlicedWriter(testData.conf, tempDir)
		for _, row := range testData.rows {
			w.Write([]byte(row))
		}
		w.Close()

		// Assert
		utils.AssertDirectoryContentsSame(t, rootDir+"/fixtures/"+testData.expectedPath, tempDir)
	}
}

func getReadCsvTestData() []testData {
	return []testData{
		{
			expectedPath: "empty",
			expectedErr:  nil,
			conf:         &config.Config{Parameters: config.Parameters{Mode: config.ModeBytes, BytesPerSlice: 1000}},
			rows:         nil,
		},
		{
			expectedPath: "empty_with_new_line",
			expectedErr:  nil,
			conf:         &config.Config{Parameters: config.Parameters{Mode: config.ModeBytes, BytesPerSlice: 1000}},
			rows: []string{
				"\n",
			},
		},
		{
			expectedPath: "one_row",
			expectedErr:  nil,
			conf:         &config.Config{Parameters: config.Parameters{Mode: config.ModeBytes, BytesPerSlice: 1000}},
			rows: []string{
				"\"abc\",\"def\"\n",
			},
		},
		{
			expectedPath: "two_rows",
			expectedErr:  nil,
			conf:         &config.Config{Parameters: config.Parameters{Mode: config.ModeBytes, BytesPerSlice: 1000}},
			rows: []string{
				"\"abc\",\"def\"\n",
				"\"123\",\"456\"\n",
			},
		},
		{
			expectedPath: "escaping",
			expectedErr:  nil,
			conf:         &config.Config{Parameters: config.Parameters{Mode: config.ModeBytes, BytesPerSlice: 1000}},
			rows: []string{
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
		{
			expectedPath: "multiple_parts_bytes_mode",
			expectedErr:  nil,
			conf:         &config.Config{Parameters: config.Parameters{Mode: config.ModeBytes, BytesPerSlice: 40}},
			rows: []string{
				"\"1bc\",\"def\"\n",
				"\"2bc\",\"def\"\n",
				"\"3bc\",\"def\"\n",
				"\"4bc\",\"def\"\n",
				"\"5bc\",\"def\"\n",
				"\"6bc\",\"def\"\n",
				"\"7bc\",\"def\"\n",
			},
		},
		{
			expectedPath: "multiple_parts_rows_mode",
			expectedErr:  nil,
			conf:         &config.Config{Parameters: config.Parameters{Mode: config.ModeRows, RowsPerSlice: 3}},
			rows: []string{
				"\"1bc\",\"def\"\n",
				"\"2bc\",\"def\"\n",
				"\"3bc\",\"def\"\n",
				"\"4bc\",\"def\"\n",
				"\"5bc\",\"def\"\n",
				"\"6bc\",\"def\"\n",
				"\"7bc\",\"def\"\n",
			},
		},
	}
}
