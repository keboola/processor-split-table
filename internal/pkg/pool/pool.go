// Package pool provides reusing of buffered and GZIP readers and writers to optimize memory usage.
package pool

import (
	"bufio"
	"io"
	"runtime"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/pgzip"
)

type BufferWriterPool struct {
	pool *sync.Pool
}

type ReadAheadBuffersPool struct {
	pool *sync.Pool
}

type GZIPReaderPool struct {
	pool *sync.Pool
}

type GZIPWriterPool struct {
	pool *sync.Pool
}

func BufferedWriters(size datasize.ByteSize) *BufferWriterPool {
	return &BufferWriterPool{
		pool: &sync.Pool{
			New: func() any {
				return bufio.NewWriterSize(nil, int(size.Bytes()))
			},
		},
	}
}

func ReadAheadBuffers(buffers int, size datasize.ByteSize) *ReadAheadBuffersPool {
	return &ReadAheadBuffersPool{
		pool: &sync.Pool{
			New: func() any {
				out := make([][]byte, buffers)
				for i := 0; i < buffers; i++ {
					out[i] = make([]byte, size)
				}
				return &out
			},
		},
	}
}

func GZIPReaders() *GZIPReaderPool {
	return &GZIPReaderPool{
		pool: &sync.Pool{
			New: func() any {
				return &pgzip.Reader{}
			},
		},
	}
}

func GZIPWriters(level int, blockSize datasize.ByteSize, blocks int) *GZIPWriterPool {
	// Use threads count as default concurrency value
	if blocks == 0 {
		blocks = runtime.GOMAXPROCS(0)
	}

	return &GZIPWriterPool{
		pool: &sync.Pool{
			New: func() any {
				w, err := pgzip.NewWriterLevel(nil, level)
				if err != nil {
					panic(err)
				}
				err = w.SetConcurrency(int(blockSize.Bytes()), blocks)
				if err != nil {
					panic(err)
				}
				return w
			},
		},
	}
}

// WriterTo gets writer from the pool.
func (p *BufferWriterPool) WriterTo(w io.Writer) *bufio.Writer {
	out := p.pool.Get().(*bufio.Writer)
	out.Reset(w)
	return out
}

// Put adds buffers back to the pool.
func (p *ReadAheadBuffersPool) Put(v *[][]byte) {
	p.pool.Put(v)
}

// Get gets buffer from the pool.
func (p *ReadAheadBuffersPool) Get() *[][]byte {
	// Reset is not needed, it is handled by the readahead library
	return p.pool.Get().(*[][]byte)
}

// Put adds writer back to the pool.
func (p *BufferWriterPool) Put(w *bufio.Writer) {
	p.pool.Put(w)
}

// ReaderFrom gets reader from the pool.
func (p *GZIPReaderPool) ReaderFrom(r io.Reader) (out *pgzip.Reader, err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil && err == nil {
			if panicErr, ok := panicValue.(error); ok {
				err = panicErr
			}
		}
	}()

	out = p.pool.Get().(*pgzip.Reader)
	if err := out.Reset(r); err != nil {
		return nil, err
	}

	return out, nil
}

// Put adds reader back to the pool.
func (p *GZIPReaderPool) Put(r *pgzip.Reader) {
	p.pool.Put(r)
}

// WriterTo gets writer from the pool.
func (p *GZIPWriterPool) WriterTo(w io.Writer) (out *pgzip.Writer, err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil && err == nil {
			if panicErr, ok := panicValue.(error); ok {
				err = panicErr
			}
		}
	}()

	out = p.pool.Get().(*pgzip.Writer)
	out.Reset(w)

	return out, nil
}

// Put adds writer back to the pool.
func (p *GZIPWriterPool) Put(w *pgzip.Writer) {
	p.pool.Put(w)
}
