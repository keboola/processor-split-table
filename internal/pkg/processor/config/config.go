package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
)

const (
	ModeBytes Mode = iota + 1
	ModeRows
	ModeSlices
)

type Mode uint

type Config struct {
	Parameters Parameters `json:"parameters" validate:"required"`
}

type Parameters struct {
	Mode             Mode   `json:"mode" validate:"required"`
	BytesPerSlice    uint64 `json:"bytesPerSlice" validate:"min=1"`
	RowsPerSlice     uint64 `json:"rowsPerSlice" validate:"min=1"`
	NumberOfSlices   uint32 `json:"numberOfSlices" validate:"min=1"`
	MinBytesPerSlice uint64 `json:"minBytesPerSlice" validate:"min=1"` // if Mode = ModeSlices
	Gzip             bool   `json:"gzip"`
	GzipLevel        int    `json:"gzipLevel" validate:"min=1,max=9"`
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

func LoadConfig(configPath string) (cfg *Config, err error) {
	// Open config
	f, err := os.OpenFile(configPath, os.O_RDONLY, 0o640)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, kbc.UserErrorf("config file not found")
		} else {
			return nil, kbc.UserErrorf("cannot open config file: %w", err)
		}
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf(`cannot close file "%s": %w`, configPath, err)
		}
	}()

	// Read config
	content, err := io.ReadAll(f)
	if err != nil {
		return nil, kbc.UserErrorf(`cannot read config "%s": %w`, configPath, err)
	}

	// Default values
	conf := &Config{
		Parameters: Parameters{
			Mode:             ModeBytes,
			BytesPerSlice:    524_288_000, // 500 MiB
			RowsPerSlice:     1_000_000,
			NumberOfSlices:   60,
			MinBytesPerSlice: 4194304, // 4 MiB
			Gzip:             true,
			GzipLevel:        2, // 1 - BestSpeed, 9 - BestCompression
		},
	}

	// Parse JSON
	err = json.Unmarshal(content, conf)
	if err != nil {
		return nil, kbc.UserErrorf("invalid configuration: %s", processJSONError(err))
	}

	// Validate
	if err := validate(conf); err != nil {
		return nil, kbc.UserErrorf("invalid configuration: %s", processJSONError(err))
	}

	return conf, nil
}

func validate(conf *Config) error {
	val := validator.New()
	val.RegisterTagNameFunc(func(fld reflect.StructField) string {
		// Use JSON field name in error messages
		return strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
	})

	if err := val.Struct(conf); err != nil {
		// nolint: errorlint
		return processValidateError(err.(validator.ValidationErrors))
	}

	return nil
}

func processJSONError(e error) string {
	// Custom error message
	var typeError *json.UnmarshalTypeError
	if errors.As(e, &typeError) {
		return fmt.Sprintf(`key "%s" has invalid type "%s"`, typeError.Field, typeError.Value)
	}
	return e.Error()
}

func processValidateError(err validator.ValidationErrors) error {
	msg := ""
	for _, e := range err {
		path := strings.TrimPrefix(e.Namespace(), "Config.")
		msg += fmt.Sprintf(
			`key="%s", value="%v" failed on the "%s" validation `,
			path,
			e.Value(),
			e.ActualTag(),
		)

		// Print only one error
		break
	}

	return errors.New(strings.TrimSpace(msg))
}
