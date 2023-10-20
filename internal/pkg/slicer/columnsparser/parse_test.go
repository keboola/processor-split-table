package columnsparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testData struct {
	comment         string
	input           []byte
	expectedColumns []string
	expectedErr     string
}

func TestParse(t *testing.T) {
	t.Parallel()

	for _, data := range GetTestParseHeaderData() {
		parser := NewParser(',', '"')
		columns, err := parser.Parse(data.input)
		assert.Equal(t, data.expectedColumns, columns, data.comment)
		if data.expectedErr != "" && assert.Error(t, err) {
			assert.Equal(t, data.expectedErr, err.Error(), data.comment)
		}
	}
}

func GetTestParseHeaderData() []testData {
	return []testData{
		{
			comment:         "Empty row",
			input:           []byte(""),
			expectedColumns: nil,
		},
		{
			comment:         "Columns",
			input:           []byte("\"a\",\"a\"\"\nb\",\"abc\""),
			expectedColumns: []string{"a", "a\"\nb", "abc"},
		},
		{
			comment:         "Columns - tolerate missing enclosure",
			input:           []byte("a,ab,abc"),
			expectedColumns: []string{"a", "ab", "abc"},
		},
		{
			comment:         "Column",
			input:           []byte("\"abc\""),
			expectedColumns: []string{"abc"},
		},
		{
			comment:         "Column, delimiter",
			input:           []byte("\"abc\","),
			expectedColumns: []string{"abc", ""},
		},
		{
			comment:         "Column, new line",
			input:           []byte("\"abc\"\n"),
			expectedColumns: []string{"abc"},
		},
		{
			comment:         "Column[with escaped enclosure + new line]",
			input:           []byte("\"a\"\"b\nc\""),
			expectedColumns: []string{"a\"b\nc"},
		},
		{
			comment:         "Column[with escaped enclosure + new line], delimiter",
			input:           []byte("\"a\"\"b\nc\","),
			expectedColumns: []string{"a\"b\nc", ""},
		},
		{
			comment:         "Column[with escaped enclosure + new line], new line",
			input:           []byte("\"a\"\"b\nc\"\n"),
			expectedColumns: []string{"a\"b\nc"},
		},
		{
			comment:         "Delimiter",
			input:           []byte(","),
			expectedColumns: []string{"", ""},
		},
		{
			comment:         "Unfinished enclosure - 1",
			input:           []byte("\""),
			expectedColumns: nil,
			expectedErr:     "reached end of the row, but enclosure is not ended",
		},
		{
			comment:         "Unfinished enclosure - 2",
			input:           []byte("\"aaa"),
			expectedColumns: nil,
			expectedErr:     "reached end of the row, but enclosure is not ended",
		},
		{
			comment:         "Unfinished enclosure - 3",
			input:           []byte("\"aaa\",\"bbb"),
			expectedColumns: nil,
			expectedErr:     "reached end of the row, but enclosure is not ended",
		},
		{
			comment:         "Input before enclosure - 1",
			input:           []byte("aaa\""),
			expectedColumns: nil,
			expectedErr:     "unexpected token \"aaa\" before enclosure at position 4",
		},
		{
			comment:         "Input before enclosure - 2",
			input:           []byte("\"aaa\",bbb\""),
			expectedColumns: nil,
			expectedErr:     "unexpected token \"bbb\" before enclosure at position 10",
		},
	}
}
