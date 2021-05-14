package columnsParser

import (
	"bytes"
	"fmt"
)

type Parser struct {
	delimiter       byte
	enclosure       byte
	row             []byte
	length          int
	insideEnclosure bool
	columns         []string
	current         []byte
	index           int
}

func NewParser(delimiter byte, enclosure byte) *Parser {
	return &Parser{delimiter: delimiter, enclosure: enclosure}
}

// Parse CSV columns from row
func (p *Parser) Parse(row []byte) ([]string, error) {
	p.row = bytes.TrimRight(row, "\n")
	p.length = len(p.row)
	p.current = nil
	p.index = 0
	p.insideEnclosure = false

	// Iterate over chars
	for p.index < p.length {
		err := p.processChar(row[p.index])
		if err != nil {
			return nil, err
		}
	}

	// Flush rest if not empty row
	if p.length > 0 {
		p.flushColumn()
	}

	// Check if enclosure is ended
	if p.insideEnclosure {
		return nil, fmt.Errorf("reached end of the row, but enclosure is not ended")
	}

	return p.columns, nil
}

func (p *Parser) processChar(char byte) error {
	switch true {
	case p.isDelimiter(char):
		// Column found
		p.flushColumn()
	case p.isEnclosure(char):
		// If next char is enclosure -> escaped enclosure
		if p.isNextCharEnclosure() {
			// Write one enclosure to column value
			p.current = append(p.current, char)
			// Skip next char
			p.index += 2
			return nil
		}

		if !p.insideEnclosure && len(p.current) > 0 {
			return fmt.Errorf("unexpected token \"%s\" before enclosure at position %d", p.current, p.index+1)
		}

		// Invert state
		p.insideEnclosure = !p.insideEnclosure
	default:
		// Char is part of the column value
		p.current = append(p.current, char)

	}

	p.index++

	return nil
}

func (p *Parser) flushColumn() {
	p.columns = append(p.columns, string(p.current))
	p.current = nil
}

func (p *Parser) isDelimiter(char byte) bool {
	// Equal to configured delimiter? Ignored inside enclosure.
	return !p.insideEnclosure && char == p.delimiter
}

func (p *Parser) isEnclosure(char byte) bool {
	// Equal to configured enclosure?
	return char == p.enclosure
}

func (p *Parser) isNextCharEnclosure() bool {
	nextIndex := p.index + 1
	nextExists := nextIndex < p.length
	return nextExists && p.row[nextIndex] == p.enclosure
}
