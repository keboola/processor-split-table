// Package config provides slicing configuration.
package config

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/c2h5oh/datasize"
)

const (
	ModeBytes Mode = iota + 1
	ModeRows
	ModeSlices
)

type Mode uint

type ByteSize = datasize.ByteSize

type Config struct {
	Mode Mode `json:"mode" mapstructure:"mode" validate:"required"`

	// Mode: bytes
	BytesPerSlice datasize.ByteSize `json:"bytesPerSlice" mapstructure:"bytes-per-slice" validate:"min=1"`

	// Mode: rows
	RowsPerSlice uint64 `json:"rowsPerSlice" mapstructure:"rows-per-slice" validate:"min=1"`

	// Mode: slices
	NumberOfSlices   uint32            `json:"numberOfSlices" mapstructure:"number-of-slices" validate:"min=1"`
	MinBytesPerSlice datasize.ByteSize `json:"minBytesPerSlice" mapstructure:"min-bytes-per-slice" validate:"min=1"` // if Mode = ModeSlices

	// Read ahead configuration
	// AheadSlices specifies number of slices opened ahead.
	AheadSlices uint32 `json:"aheadSlices" mapstructure:"ahead-slices" validate:"min=1"`
	// AheadSlices specifies number of blocks read ahead from a slice.
	AheadBlocks uint32 `json:"aheadBlocks" mapstructure:"ahead-blocks"` // 0 disables read-ahead
	// AheadBlockSize specifies size of a one read ahead block.
	AheadBlockSize datasize.ByteSize `json:"aheadBlockSize" mapstructure:"ahead-block-size" validate:"min=32768"` // min 32KB

	// GZIP configuration
	Gzip            bool              `json:"gzip" mapstructure:"gzip"`
	GzipLevel       int               `json:"gzipLevel" mapstructure:"gzip-level" validate:"min=1,max=9"`
	GzipConcurrency uint32            `json:"gzipConcurrency" mapstructure:"gzip-concurrency"`                   // 0 means auto = number of CPU threads
	GzipBlockSize   datasize.ByteSize `json:"gzipBlockSize" mapstructure:"gzip-block-size" validate:"min=32768"` // min 32KB

	// BufferSize is used if GZIP is disabled.
	// If Gzip is enabled, the total buffer size is GzipConcurrency * GzipBlockSize.
	BufferSize datasize.ByteSize `json:"bufferSize" mapstructure:"buffer-size" validate:"min=32768"`
}

func Default() Config {
	return Config{
		Mode:             ModeBytes,
		BytesPerSlice:    500 * datasize.MB,
		RowsPerSlice:     1_000_000,
		NumberOfSlices:   60,
		MinBytesPerSlice: 4 * datasize.MB,
		AheadSlices:      1,
		AheadBlocks:      16,
		AheadBlockSize:   1 * datasize.MB,
		Gzip:             true,
		GzipLevel:        1,                // 1 - BestSpeed, 9 - BestCompression
		GzipConcurrency:  0,                // 0 = auto = number of CPU threads
		GzipBlockSize:    1 * datasize.MB,  // so total buffer size is by default: GzipConcurrency (number of CPU threads) * GzipBlockSize
		BufferSize:       20 * datasize.MB, // it is used if GZIP is disabled
	}
}

func (v *Config) UnmarshalJSON(data []byte) error {
	// Decode to a map
	m := make(map[string]any)
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	// Convert bytes as int, to bytes as string, for example 123 -> "123B".
	// This ensures backward compatibility with older configurations.
	if i, ok := m["bytesPerSlice"].(float64); ok {
		m["bytesPerSlice"] = strconv.FormatInt(int64(i), 10) + "B"
	}
	if i, ok := m["minBytesPerSlice"].(float64); ok {
		m["minBytesPerSlice"] = strconv.FormatInt(int64(i), 10) + "B"
	}

	// Encode update version
	data, err = json.Marshal(m)
	if err != nil {
		return err
	}

	// Decode to the struct, skip UnmarshalJSON implementation
	type _c Config
	return json.Unmarshal(data, (*_c)(v))
}

func (m Mode) String() string {
	str, err := m.StringOrErr()
	if err != nil {
		panic(err)
	}
	return str
}

func (m Mode) StringOrErr() (string, error) {
	switch m {
	case ModeBytes:
		return "bytes", nil
	case ModeRows:
		return "rows", nil
	case ModeSlices:
		return "slices", nil
	default:
		return "", fmt.Errorf(`unexpected value "%v" for "mode"`, m)
	}
}

func (m Mode) MarshalText() ([]byte, error) {
	str, err := m.StringOrErr()
	return []byte(str), err
}

func (m *Mode) UnmarshalText(b []byte) error {
	// Convert "mode" string value to numeric constant
	str := string(b)
	switch str {
	case "bytes":
		*m = ModeBytes
	case "rows":
		*m = ModeRows
	case "slices":
		*m = ModeSlices
	default:
		return fmt.Errorf(`unexpected value "%s" for "mode", use "rows", "bytes" or "slices"`, str)
	}

	return nil
}
