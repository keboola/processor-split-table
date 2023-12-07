// Package progress provides slicing progress logger.
// An input reader can be wrapped by the Logger.NewMeter method.
package progress

import (
	"io"
	"sync"

	clock "github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/processor-split-table/internal/pkg/log"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

type Logger struct {
	logger  log.Logger
	total   datasize.ByteSize
	read    datasize.ByteSize
	lock    *sync.Mutex
	timer   *clock.Timer
	backoff *backoff.ExponentialBackOff
	message string
}

func NewLogger(clk clock.Clock, logger log.Logger, interval config.LogIntervalConfig, total datasize.ByteSize, message string) *Logger {
	r := &Logger{
		logger:  logger,
		total:   total,
		lock:    &sync.Mutex{},
		backoff: newBackoff(interval),
		message: message,
	}

	// Schedule the first log message.
	// Log message is generated with exponential delay between configured minimum and maximum.
	// It works well for small, medium and large files.
	r.timer = clk.AfterFunc(interval.Initial, r.log)

	return r
}

func (r *Logger) NewMeter(reader io.Reader) io.Reader {
	return &meter{Logger: r, reader: reader}
}

func (r *Logger) Close() error {
	r.timer.Stop()
	return nil
}

func (r *Logger) log() {
	r.lock.Lock()
	r.logger.Infof(`%s %05.2f%%`, r.message, float64(r.read*100)/float64(r.total))
	r.lock.Unlock()

	// Schedule next log message
	r.timer.Reset(r.backoff.NextBackOff())
}

type meter struct {
	*Logger
	reader io.Reader
}

func (r *meter) Read(b []byte) (n int, err error) {
	n, err = r.reader.Read(b)
	if err == nil && n != 0 {
		r.lock.Lock()
		r.read += datasize.ByteSize(n)
		r.lock.Unlock()
	}
	return
}

func newBackoff(interval config.LogIntervalConfig) *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.Multiplier = interval.Multiplier
	b.InitialInterval = interval.Initial
	b.MaxInterval = interval.Maximum
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}
