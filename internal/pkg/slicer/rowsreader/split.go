package rowsreader

import "bufio"

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
