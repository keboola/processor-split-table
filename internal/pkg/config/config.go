package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
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
		return fmt.Errorf("unexpected value \"%s\" for \"mode\". Use \"rows\", \"bytes\" or \"slices\"", str)
	}

	return nil
}

func LoadConfig(configPath string) *Config {
	// Load file
	f, err := os.OpenFile(configPath, os.O_RDONLY, 0o640)
	if err != nil {
		if os.IsNotExist(err) {
			kbc.PanicUserErrorf("Config file not found.")
		} else {
			kbc.PanicUserErrorf("Cannot open config file: %s", err)
		}
	}
	content := utils.ReadAll(f, configPath)
	utils.CloseFile(f, configPath)

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
		kbc.PanicUserErrorf("Invalid configuration: %s.", processJSONError(err))
	}

	// Validate
	validate(conf)

	return conf
}

func validate(conf *Config) {
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		// Use JSON field name in error messages
		return strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
	})

	err := validate.Struct(conf)
	if err != nil {
		// nolint: errorlint
		kbc.PanicUserErrorf("Invalid configuration: %s.", processValidateError(err.(validator.ValidationErrors)))
	}
}

func processJSONError(e error) string {
	// Custom error message
	var typeError *json.UnmarshalTypeError
	if errors.As(e, &typeError) {
		return fmt.Sprintf("key \"%s\" has invalid type \"%s\"", typeError.Field, typeError.Value)
	}

	return e.Error()
}

func processValidateError(err validator.ValidationErrors) string {
	msg := ""
	for _, e := range err {
		path := strings.TrimPrefix(e.Namespace(), "Config.")
		msg += fmt.Sprintf(
			"key=\"%s\", value=\"%v\" failed on the \"%s\" validation ",
			path,
			e.Value(),
			e.ActualTag(),
		)

		// Print only one error
		break
	}

	return strings.TrimSpace(msg)
}
