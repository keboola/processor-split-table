package closer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClosers(t *testing.T) {
	t.Parallel()

	var log bytes.Buffer

	var c Closers
	c.Append(func() error {
		log.WriteString("close1\n")
		return nil
	})
	c.Append(func() error {
		log.WriteString("close2\n")
		return errors.New("some error")
	})
	c.Append(func() error {
		log.WriteString("close3\n")
		return nil
	})
	c.Append(func() error {
		log.WriteString("close4\n")
		return nil
	})

	err := c.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "some error", err.Error())
	}

	assert.Equal(t, "close4\nclose3\nclose2\n", log.String())
}
