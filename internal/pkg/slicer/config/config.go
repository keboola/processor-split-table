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
	Mode             Mode              `json:"mode" validate:"required"`
	BytesPerSlice    datasize.ByteSize `json:"bytesPerSlice" validate:"min=1"`
	RowsPerSlice     uint64            `json:"rowsPerSlice" validate:"min=1"`
	NumberOfSlices   uint32            `json:"numberOfSlices" validate:"min=1"`
	MinBytesPerSlice datasize.ByteSize `json:"minBytesPerSlice" validate:"min=1"` // if Mode = ModeSlices
	Gzip             bool              `json:"gzip"`
	GzipLevel        int               `json:"gzipLevel" validate:"min=1,max=9"`
}

func DefaultConfig() Config {
	return Config{
		Mode:             ModeBytes,
		BytesPerSlice:    500 * datasize.MB,
		RowsPerSlice:     1_000_000,
		NumberOfSlices:   60,
		MinBytesPerSlice: 4 * datasize.MB,
		Gzip:             true,
		GzipLevel:        2, // 1 - BestSpeed, 9 - BestCompression
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

func (m Mode) MarshalText() ([]byte, error) {
	switch m {
	case ModeBytes:
		return []byte("bytes"), nil
	case ModeRows:
		return []byte("rows"), nil
	case ModeSlices:
		return []byte("slices"), nil
	default:
		return nil, fmt.Errorf(`unexpected value "%v" for "mode"`, m)
	}
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
