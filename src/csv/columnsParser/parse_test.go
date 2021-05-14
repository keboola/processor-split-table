package columnsParser

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testData struct {
	comment         string
	input           []byte
	expectedColumns []string
	expectedErr     error
}

func TestParse(t *testing.T) {
	for _, data := range GetTestParseHeaderData() {
		parser := NewParser(',', '"')
		columns, err := parser.Parse(data.input)
		assert.Equal(t, data.expectedColumns, columns, data.comment)
		assert.Equal(t, data.expectedErr, err, data.comment)
	}
}

func GetTestParseHeaderData() []testData {
	return []testData{
		{
			comment:         "Empty row",
			input:           []byte(""),
			expectedColumns: nil,
			expectedErr:     nil,
		},
		{
			comment:         "Columns",
			input:           []byte("\"a\",\"a\"\"\nb\",\"abc\""),
			expectedColumns: []string{"a", "a\"\nb", "abc"},
			expectedErr:     nil,
		},
		{
			comment:         "Columns - tolerate missing enclosure",
			input:           []byte("a,ab,abc"),
			expectedColumns: []string{"a", "ab", "abc"},
			expectedErr:     nil,
		},
		{
			comment:         "Column",
			input:           []byte("\"abc\""),
			expectedColumns: []string{"abc"},
			expectedErr:     nil,
		},
		{
			comment:         "Column, delimiter",
			input:           []byte("\"abc\","),
			expectedColumns: []string{"abc", ""},
			expectedErr:     nil,
		},
		{
			comment:         "Column, new line",
			input:           []byte("\"abc\"\n"),
			expectedColumns: []string{"abc"},
			expectedErr:     nil,
		},
		{
			comment:         "Column[with escaped enclosure + new line]",
			input:           []byte("\"a\"\"b\nc\""),
			expectedColumns: []string{"a\"b\nc"},
			expectedErr:     nil,
		},
		{
			comment:         "Column[with escaped enclosure + new line], delimiter",
			input:           []byte("\"a\"\"b\nc\","),
			expectedColumns: []string{"a\"b\nc", ""},
			expectedErr:     nil,
		},
		{
			comment:         "Column[with escaped enclosure + new line], new line",
			input:           []byte("\"a\"\"b\nc\"\n"),
			expectedColumns: []string{"a\"b\nc"},
			expectedErr:     nil,
		},
		{
			comment:         "Delimiter",
			input:           []byte(","),
			expectedColumns: []string{"", ""},
			expectedErr:     nil,
		},
		{
			comment:         "Unfinished enclosure - 1",
			input:           []byte("\""),
			expectedColumns: nil,
			expectedErr:     fmt.Errorf("reached end of the row, but enclosure is not ended"),
		},
		{
			comment:         "Unfinished enclosure - 2",
			input:           []byte("\"aaa"),
			expectedColumns: nil,
			expectedErr:     fmt.Errorf("reached end of the row, but enclosure is not ended"),
		},
		{
			comment:         "Unfinished enclosure - 3",
			input:           []byte("\"aaa\",\"bbb"),
			expectedColumns: nil,
			expectedErr:     fmt.Errorf("reached end of the row, but enclosure is not ended"),
		},
		{
			comment:         "Input before enclosure - 1",
			input:           []byte("aaa\""),
			expectedColumns: nil,
			expectedErr:     fmt.Errorf("unexpected token \"aaa\" before enclosure at position 4"),
		},
		{
			comment:         "Input before enclosure - 2",
			input:           []byte("\"aaa\",bbb\""),
			expectedColumns: nil,
			expectedErr:     fmt.Errorf("unexpected token \"bbb\" before enclosure at position 10"),
		},
	}
}
