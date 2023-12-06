package pool

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferWriters(t *testing.T) {
	t.Parallel()

	rawData := []byte("foo")

	pool := BufferedWriters(100 * datasize.KB)

	var out1 bytes.Buffer
	w1 := pool.WriterTo(&out1)
	_, err := w1.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w1.Flush())
	require.Equal(t, rawData, out1.Bytes())

	var out2 bytes.Buffer
	w2 := pool.WriterTo(&out2)
	require.NotSame(t, w1, w2)
	_, err = w2.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w2.Flush())
	require.Equal(t, rawData, out2.Bytes())

	// Put the writers back to the pool
	pool.Put(w1)
	pool.Put(w2)

	// Writer is reused (w1), but it cannot be asserted
	var out3 bytes.Buffer
	w3 := pool.WriterTo(&out3)
	_, err = w3.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w3.Flush())
	require.Equal(t, rawData, out3.Bytes())

	// Writer is reused (w2), but it cannot be asserted
	var out4 bytes.Buffer
	w4 := pool.WriterTo(&out4)
	require.NotSame(t, w3, w4)
	_, err = w4.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w4.Flush())
	require.Equal(t, rawData, out4.Bytes())
}

func TestReadAheadBuffers(t *testing.T) {
	t.Parallel()

	buffersCount := 5
	bufferSize := 2 * datasize.MB
	pool := ReadAheadBuffers(buffersCount, bufferSize)

	buffers := pool.Get()
	assert.Equal(t, buffersCount, len(*buffers))
	for _, buffer := range *buffers {
		assert.Equal(t, int(bufferSize), len(buffer))
	}

	pool.Put(buffers)
}

func TestGZIPReaders(t *testing.T) {
	t.Parallel()

	rawData := []byte("foo")
	gzipped := gzipData(t, rawData)

	pool := GZIPReaders()

	r1, err := pool.ReaderFrom(bytes.NewReader(gzipped))
	require.NoError(t, err)
	bytes1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.NoError(t, r1.Close())
	require.Equal(t, rawData, bytes1)

	r2, err := pool.ReaderFrom(bytes.NewReader(gzipped))
	require.NoError(t, err)
	require.NotSame(t, r1, r2)
	bytes2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.NoError(t, r2.Close())
	require.Equal(t, rawData, bytes2)

	// Put the readers back to the pool
	pool.Put(r1)
	pool.Put(r2)

	// Reader is reused (r1), but it cannot be asserted
	r3, err := pool.ReaderFrom(bytes.NewReader(gzipped))
	require.NoError(t, err)
	bytes3, err := io.ReadAll(r3)
	require.NoError(t, err)
	require.NoError(t, r3.Close())
	require.Equal(t, rawData, bytes3)

	// Reader is reused (r4), but it cannot be asserted
	r4, err := pool.ReaderFrom(bytes.NewReader(gzipped))
	require.NoError(t, err)
	require.NotSame(t, r3, r4)
	bytes4, err := io.ReadAll(r4)
	require.NoError(t, err)
	require.NoError(t, r4.Close())
	require.Equal(t, rawData, bytes4)
}

func TestGZIPWriters(t *testing.T) {
	t.Parallel()

	rawData := []byte("foo")

	pool := GZIPWriters(2, 100*datasize.KB, 0)

	var out1 bytes.Buffer
	w1, err := pool.WriterTo(&out1)
	require.NoError(t, err)
	_, err = w1.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w1.Close())
	require.Equal(t, rawData, unGzipData(t, out1.Bytes()))

	var out2 bytes.Buffer
	w2, err := pool.WriterTo(&out2)
	require.NoError(t, err)
	require.NotSame(t, w1, w2)
	_, err = w2.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w2.Close())
	require.Equal(t, rawData, unGzipData(t, out2.Bytes()))

	// Put the writers back to the pool
	pool.Put(w1)
	pool.Put(w2)

	// Writer is reused (w1), but it cannot be asserted
	var out3 bytes.Buffer
	w3, err := pool.WriterTo(&out3)
	require.NoError(t, err)
	_, err = w3.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w3.Close())
	require.Equal(t, rawData, unGzipData(t, out3.Bytes()))

	// Writer is reused (w2), but it cannot be asserted
	var out4 bytes.Buffer
	w4, err := pool.WriterTo(&out4)
	require.NoError(t, err)
	require.NotSame(t, w3, w4)
	_, err = w4.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w4.Close())
	require.Equal(t, rawData, unGzipData(t, out4.Bytes()))
}

func gzipData(t *testing.T, rawData []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	_, err := w.Write(rawData)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return out.Bytes()
}

func unGzipData(t *testing.T, gzipped []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(gzipped))
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return out
}
