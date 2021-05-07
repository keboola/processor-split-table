package config

import (
	"encoding/json"
	"fmt"
	"github.com/go-playground/validator/v10"
	"keboola.processor-split-by-nrows/src/kbc"
	"keboola.processor-split-by-nrows/src/utils"
	"os"
	"reflect"
	"strings"
)

const (
	ModeBytes Mode = iota + 1
	ModeRows
)

type Mode uint

type Config struct {
	Parameters Parameters `json:"parameters" validate:"required"`
}

type Parameters struct {
	Mode          Mode   `json:"mode" validate:"required"`
	BytesPerSlice uint64 `json:"bytesPerSlice" validate:"required"`
	RowsPerSlice  uint64 `json:"rowsPerSlice" validate:"required"`
}

func (m *Mode) UnmarshalText(b []byte) error {
	// Convert "mode" string value to numeric constant
	str := string(b)
	switch str {
	case "bytes":
		*m = ModeBytes
	case "rows":
		*m = ModeRows
	default:
		return fmt.Errorf("unexpected value \"%s\" for \"mode\". Use \"rows\" or \"bytes\"", str)
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
	content := utils.ReadAllFromFile(f, configPath)
	utils.CloseFile(f, configPath)

	// Default values
	conf := &Config{
		Parameters: Parameters{
			Mode:          ModeBytes,
			BytesPerSlice: 524_288_000, // 500 MiB
			RowsPerSlice:  1_000_000,
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
			"Key \"%s\" failed on the \"%s\" validation. ",
			path,
			e.ActualTag(),
		)

		// Print only one error
		break
	}

	return strings.TrimSpace(msg)
}
