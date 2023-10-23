// Package config provides slicing configuration.
package config

import "fmt"

const (
	ModeBytes Mode = iota + 1
	ModeRows
	ModeSlices
)

type Mode uint

type Config struct {
	Mode             Mode   `json:"mode" validate:"required"`
	BytesPerSlice    uint64 `json:"bytesPerSlice" validate:"min=1"`
	RowsPerSlice     uint64 `json:"rowsPerSlice" validate:"min=1"`
	NumberOfSlices   uint32 `json:"numberOfSlices" validate:"min=1"`
	MinBytesPerSlice uint64 `json:"minBytesPerSlice" validate:"min=1"` // if Mode = ModeSlices
	Gzip             bool   `json:"gzip"`
	GzipLevel        int    `json:"gzipLevel" validate:"min=1,max=9"`
}

func DefaultConfig() Config {
	return Config{
		Mode:             ModeBytes,
		BytesPerSlice:    524_288_000, // 500 MiB
		RowsPerSlice:     1_000_000,
		NumberOfSlices:   60,
		MinBytesPerSlice: 4194304, // 4 MiB
		Gzip:             true,
		GzipLevel:        2, // 1 - BestSpeed, 9 - BestCompression
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
