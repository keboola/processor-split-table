package config

import (
	"encoding/json"
	"fmt"
	"github.com/go-playground/validator/v10"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/utils"
	"os"
	"reflect"
	"strings"
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
	f, err := os.OpenFile(configPath, os.O_RDONLY, 0640)
	if err != nil {
		if os.IsNotExist(err) {
			kbc.PanicUserError("Config file not found.")
		} else {
			kbc.PanicUserError("Cannot open config file: %s", err)
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
		kbc.PanicUserError("Invalid configuration: %s.", processJsonError(err))
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
		kbc.PanicUserError("Invalid configuration: %s.", processValidateError(err.(validator.ValidationErrors)))
	}
}

func processJsonError(e error) string {
	switch e := e.(type) {
	// Custom error message
	case *json.UnmarshalTypeError:
		return fmt.Sprintf("key \"%s\" has invalid type \"%s\"", e.Field, e.Value)
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
