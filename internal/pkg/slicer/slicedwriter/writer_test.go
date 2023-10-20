package slicedwriter

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

type testData struct {
	config       config.Config
	rows         []string
	expectedErr  error
	expectedPath string
}

func TestNewSlicedWriter(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	// Config
	cfg := config.Config{
		Mode:          config.ModeBytes,
		BytesPerSlice: 123,
	}

	// Create writer
	w, err := New(cfg, 1000, tempDir)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, tempDir, w.outPath)
	assert.Equal(t, uint32(1), w.sliceNumber)          // <<<<<<
	assert.Equal(t, tempDir+"/part0001", w.slice.path) // <<<<<<
	assert.NotNil(t, w.slice.out)
	assert.Equal(t, uint64(0), w.slice.rows)
	assert.Equal(t, uint64(0), w.slice.rows)
	assert.Equal(t, uint64(0), w.allRows)
	assert.Equal(t, datasize.ByteSize(0), w.allBytes)
}

func TestCreateNextSlice(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	// Config
	cfg := config.Config{
		Mode:          config.ModeBytes,
		BytesPerSlice: 123,
	}
	// Create writer
	w, err := New(cfg, 1000, tempDir)
	require.NoError(t, err)

	require.NoError(t, w.createNextSlice())

	// Assert
	assert.Equal(t, tempDir, w.outPath)
	assert.Equal(t, uint32(2), w.sliceNumber)          // <<<<<<
	assert.Equal(t, tempDir+"/part0002", w.slice.path) // <<<<<<
	assert.NotNil(t, w.slice.out)
	assert.Equal(t, uint64(0), w.slice.rows)
	assert.Equal(t, uint64(0), w.slice.rows)
	assert.Equal(t, uint64(0), w.allRows)
	assert.Equal(t, datasize.ByteSize(0), w.allBytes)
}

func TestIsSpaceForNextRowBytes(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	// Config
	cfg := config.Config{
		Mode:          config.ModeBytes,
		BytesPerSlice: 123, // <<<<<<
	}

	// Create writer
	w, err := New(cfg, 1000, tempDir)
	require.NoError(t, err)
	w.allRows = 10
	w.allBytes = 200
	w.slice.rows = 5
	w.slice.bytes = 100 // <<<<<< 23 bytes left

	// Assert
	assert.True(t, w.slice.IsSpaceForNextRow(22))
	assert.True(t, w.slice.IsSpaceForNextRow(23))
	assert.False(t, w.slice.IsSpaceForNextRow(24))
	assert.False(t, w.slice.IsSpaceForNextRow(25))
}

func TestIsSpaceForNextRowRows(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	// Config
	cfg := config.Config{
		Mode:         config.ModeRows,
		RowsPerSlice: 10,
	}

	// Create writer
	w, err := New(cfg, 1000, tempDir)
	require.NoError(t, err)
	w.allRows = 10
	w.allBytes = 200
	w.slice.rows = 5 // <<<<<< 5 rows left
	w.slice.bytes = 100

	// Assert
	assert.True(t, w.slice.IsSpaceForNextRow(123))
	assert.True(t, w.slice.IsSpaceForNextRow(123))
	w.slice.writer.config.RowsPerSlice = 5 // <<<<<< no row left
	assert.False(t, w.slice.IsSpaceForNextRow(123))
	assert.False(t, w.slice.IsSpaceForNextRow(123))
}

func TestBytesMode(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	// Config
	cfg := config.Config{
		Mode:           config.ModeBytes,
		BytesPerSlice:  40,
		NumberOfSlices: 2, // no effect
		RowsPerSlice:   2, // no effect
	}

	// Create writer
	w, err := New(cfg, 1000, tempDir)
	require.NoError(t, err)

	// 1 slice
	assert.NoError(t, w.Write([]byte("\"1bc\",\"def\"\n"))) // <<<<<< 12B
	assert.Equal(t, uint32(1), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"2bc\",\"def\"\n")))
	assert.Equal(t, uint32(1), w.sliceNumber) // <<<<<< 24B
	assert.NoError(t, w.Write([]byte("\"3bc\",\"def\"\n")))
	assert.Equal(t, uint32(1), w.sliceNumber) // <<<<<< 32B
	// 2 slice
	assert.NoError(t, w.Write([]byte("\"4bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber) // <<<<<< 44B -> new slice -> 12B
	assert.NoError(t, w.Write([]byte("\"5bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"6bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber)
	// 3 slice
	assert.NoError(t, w.Write([]byte("\"7bc\",\"def\"\n")))
	assert.Equal(t, uint32(3), w.sliceNumber)
}

func TestRowsMode(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	// Config
	cfg := config.Config{
		Mode:           config.ModeRows,
		RowsPerSlice:   3,
		BytesPerSlice:  5, // no effect
		NumberOfSlices: 2, // no effect
	}

	// Create writer
	w, err := New(cfg, 1000, tempDir)
	require.NoError(t, err)

	// 1 slice
	assert.NoError(t, w.Write([]byte("\"1bc\",\"def\"\n")))
	assert.Equal(t, uint32(1), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"2bc\",\"def\"\n")))
	assert.Equal(t, uint32(1), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"3bc\",\"def\"\n")))
	assert.Equal(t, uint32(1), w.sliceNumber)
	// 2 slice
	assert.NoError(t, w.Write([]byte("\"4bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"5bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"6bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber)
	// 3 slice
	assert.NoError(t, w.Write([]byte("\"7bc\",\"def\"\n")))
	assert.Equal(t, uint32(3), w.sliceNumber)
}

func TestSlicesMode(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tempDir := t.TempDir()

	// Config
	cfg := config.Config{
		Mode:             config.ModeSlices,
		NumberOfSlices:   3,
		MinBytesPerSlice: 1,
		BytesPerSlice:    1, // no effect
		RowsPerSlice:     1, // no effect
	}

	// Create writer
	w, err := New(cfg, 7*12, tempDir)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), w.config.NumberOfSlices)
	assert.Equal(t, datasize.ByteSize(28), w.config.BytesPerSlice) // 7 row * 12 bytes / 3 slices = 28 bytes per slice

	// 1 slice
	assert.NoError(t, w.Write([]byte("\"1bc\",\"def\"\n"))) // 12 bytes
	assert.Equal(t, uint32(1), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"2bc\",\"def\"\n")))
	assert.Equal(t, uint32(1), w.sliceNumber)
	// 2 slice
	assert.NoError(t, w.Write([]byte("\"3bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"4bc\",\"def\"\n")))
	assert.Equal(t, uint32(2), w.sliceNumber)
	// 3 slice
	assert.NoError(t, w.Write([]byte("\"5bc\",\"def\"\n")))
	assert.Equal(t, uint32(3), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"6bc\",\"def\"\n")))
	assert.Equal(t, uint32(3), w.sliceNumber)
	assert.NoError(t, w.Write([]byte("\"7bc\",\"def\"\n")))
	assert.Equal(t, uint32(3), w.sliceNumber)
}

func TestWriteCsv(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	for _, testData := range getReadCsvTestData() {
		tempDir := t.TempDir()
		w, err := New(testData.config, 1000, tempDir)
		require.NoError(t, err)
		for _, row := range testData.rows {
			assert.NoError(t, w.Write([]byte(row)))
		}
		assert.NoError(t, w.Close())

		// Assert
		utils.AssertDirectoryContentsSame(t, rootDir+"/fixtures/"+testData.expectedPath, tempDir)
	}
}

func getReadCsvTestData() []testData {
	return []testData{
		{
			expectedPath: "empty",
			expectedErr:  nil,
			config:       config.Config{Mode: config.ModeBytes, BytesPerSlice: 1000},
			rows:         nil,
		},
		{
			expectedPath: "empty_with_new_line",
			expectedErr:  nil,
			config:       config.Config{Mode: config.ModeBytes, BytesPerSlice: 1000},
			rows: []string{
				"\n",
			},
		},
		{
			expectedPath: "one_row",
			expectedErr:  nil,
			config:       config.Config{Mode: config.ModeBytes, BytesPerSlice: 1000},
			rows: []string{
				"\"abc\",\"def\"\n",
			},
		},
		{
			expectedPath: "two_rows",
			expectedErr:  nil,
			config:       config.Config{Mode: config.ModeBytes, BytesPerSlice: 1000},
			rows: []string{
				"\"abc\",\"def\"\n",
				"\"123\",\"456\"\n",
			},
		},
		{
			expectedPath: "escaping",
			expectedErr:  nil,
			config:       config.Config{Mode: config.ModeBytes, BytesPerSlice: 1000},
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
			config:       config.Config{Mode: config.ModeBytes, BytesPerSlice: 40},
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
			config:       config.Config{Mode: config.ModeRows, RowsPerSlice: 3},
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
