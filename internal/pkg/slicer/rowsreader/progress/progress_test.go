package progress

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/keboola/processor-split-table/internal/pkg/log"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
)

func TestLogger(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()

	logs := &syncBuffer{}
	logger := newDebugLogger(logs)

	// Create progress logger
	interval := config.LogIntervalConfig{Multiplier: 1.5, Initial: time.Minute, Maximum: 15 * time.Minute}
	progress := NewLogger(clk, logger, interval, 3+3+5, "progress message")

	// Create 3 bytes readers with attached meters
	r1 := progress.NewMeter(bytes.NewReader([]byte("foo")))   // 3 bytes
	r2 := progress.NewMeter(bytes.NewReader([]byte("bar")))   // 3 bytes
	r3 := progress.NewMeter(bytes.NewReader([]byte("bazzz"))) // 5 bytes

	assertLogsAfter := func(after time.Duration, expected string) {
		clk.Add(after)
		expected = strings.TrimSpace(expected)
		condition := func() bool { return expected == strings.TrimSpace(logs.String()) }
		assert.Eventually(t, condition, time.Second, time.Millisecond)
		assert.Equal(t, expected, strings.TrimSpace(logs.String()))
		logs.Reset()
	}

	// There is no log message after initialization
	assert.Equal(t, "", logs.String())

	// The first message is logged after minute
	assertLogsAfter(1*time.Minute, `
INFO  progress message 00.00%
`)

	// Add 5 minutes
	assertLogsAfter(5*time.Minute, `
INFO  progress message 00.00%
INFO  progress message 00.00%
INFO  progress message 00.00%
`)

	// Read 3 bytes and add 10 minutes
	_, err := io.ReadAll(r1)
	require.NoError(t, err)
	assertLogsAfter(10*time.Minute, `
INFO  progress message 27.27%
INFO  progress message 27.27%
`)

	// Read 3 bytes and add 10 minutes
	_, err = io.ReadAll(r2)
	require.NoError(t, err)
	assertLogsAfter(10*time.Minute, `
INFO  progress message 54.55%
`)

	// Read last 5 bytes and add 10 minutes
	_, err = io.ReadAll(r3)
	require.NoError(t, err)
	assertLogsAfter(10*time.Minute, `
INFO  progress message 100.00%
`)

	// Add 30 minutes
	assertLogsAfter(30*time.Minute, `
INFO  progress message 100.00%
INFO  progress message 100.00%
`)
}

func TestBackoff(t *testing.T) {
	t.Parallel()
	b := newBackoff(config.LogIntervalConfig{Multiplier: 2, Initial: time.Minute, Maximum: 15 * time.Minute})

	var intervals []time.Duration
	for i := 0; i < 10; i++ {
		intervals = append(intervals, b.NextBackOff())
	}

	assert.Equal(t, []time.Duration{
		time.Minute,
		2 * time.Minute,
		4 * time.Minute,
		8 * time.Minute,
		15 * time.Minute,
		15 * time.Minute,
		15 * time.Minute,
		15 * time.Minute,
		15 * time.Minute,
		15 * time.Minute,
	}, intervals)
}

func newDebugLogger(out io.Writer) log.Logger {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.ConsoleSeparator = "  "
	encoderConfig.TimeKey = ""
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(out),
		zapcore.DebugLevel,
	)).Sugar()
}

type syncBuffer struct {
	buffer bytes.Buffer
	lock   sync.Mutex
}

func (s *syncBuffer) Write(p []byte) (n int, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.buffer.Write(p)
}

func (s *syncBuffer) String() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.buffer.String()
}

func (s *syncBuffer) Reset() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.buffer.Reset()
}
