// Package slicer provider slicing of an input table to an output table according to the configuration.
package slicer

import (
	"errors"
	"fmt"
	"os"

	"github.com/c2h5oh/datasize"
	"github.com/dustin/go-humanize"
	"github.com/go-playground/validator/v10"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/log"
	manifestPkg "github.com/keboola/processor-split-table/internal/pkg/manifest"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/rowsreader"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/slicedwriter"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

type Table struct {
	config.Config        `json:"config" mapstructure:",squash"`
	Name                 string `validate:"required" json:"name"  mapstructure:"table-name"`
	InPath               string `validate:"required"  json:"inPath" mapstructure:"table-input-path"`
	InManifestPath       string `json:"inManifestPath"  mapstructure:"table-input-manifest-path"`
	InManifestMustExists bool   `json:"-" mapstructure:"-"` // true in CLI, false in processor
	OutPath              string `validate:"required" json:"outPath" mapstructure:"table-output-path"`
	OutManifestPath      string `validate:"required" json:"outManifestPath" mapstructure:"table-output-manifest-path"`
}

func SliceTable(logger log.Logger, table Table) (err error) {
	// Validate
	val := validator.New()
	if err := val.Struct(table); err != nil {
		return kbc.UserErrorf(`table definition is not valid: %w`, err)
	}

	// Get input type
	stat, err := os.Stat(table.InPath)
	if errors.Is(err, os.ErrNotExist) {
		return kbc.UserErrorf(`input table "%s" not found`, table.InPath)
	} else if err != nil {
		return err
	}
	slicedInput := stat.IsDir()

	// Load manifest
	manifest, err := manifestPkg.LoadManifest(table.InManifestPath)
	if err != nil {
		return err
	}

	// Manifest must exist if the path is specified in CLI
	if table.InManifestMustExists && table.InManifestPath != "" && !manifest.Exists() {
		return fmt.Errorf(`manifest "%s" not found`, table.InManifestPath)
	}

	// Check manifest, if the table is sliced
	if slicedInput && !manifest.Exists() {
		return kbc.UserErrorf(`the manifest "%s" not found, it is required for the sliced table`, table.InManifestPath)
	}
	if slicedInput && !manifest.HasColumns() {
		return kbc.UserErrorf(`the manifest "%s" has no columns, columns are required for the sliced table`, table.InManifestPath)
	}

	// Create target dir
	logger.Infof("Slicing table \"%s\".", table.Name)
	if err := utils.Mkdir(table.OutPath); err != nil {
		return err
	}

	// Create reader
	var inputSize datasize.ByteSize
	var reader *rowsreader.Reader
	if slicedInput {
		var slices kbc.Slices
		if slices, err = kbc.FindSlices(table.InPath); err != nil {
			return err
		}
		if inputSize, err = slices.Size(); err != nil {
			return err
		}
		reader, err = rowsreader.NewSlicesReader(table.Config, table.InPath, slices, manifest.Delimiter(), manifest.Enclosure())
		if err != nil {
			return err
		}
	} else {
		inputSize = datasize.ByteSize(stat.Size())
		reader, err = rowsreader.NewFileReader(table.Config, table.InPath, manifest.Delimiter(), manifest.Enclosure())
		if err != nil {
			return err
		}
	}

	// Create writer
	writer, err := slicedwriter.New(table.Config, inputSize, table.OutPath)
	if err != nil {
		return err
	}

	// If manifest without defined columns -> store first row/header to manifest "columns" key
	addColumnsToManifest := !manifest.HasColumns()
	if addColumnsToManifest {
		if header, err := reader.Header(); err == nil {
			manifest.SetColumns(header)
		} else {
			return err
		}
	}

	// Read all rows from input table and write to sliced table
	for reader.Read() {
		if err := writer.Write(reader.Bytes()); err != nil {
			return err
		}
	}

	// Close the reader
	if err = reader.Close(); err != nil {
		return fmt.Errorf("error when reading CSV \"%s\": %w", table.InPath, err)
	}

	// Close the writer
	if err = writer.Close(); err != nil {
		return err
	}

	// Get output size
	outBytes := writer.AlLBytes()
	if writer.GzipEnabled() {
		if dirSize, err := utils.DirSize(table.OutPath); err == nil {
			outBytes = dirSize
		} else {
			return err
		}
	}

	// Write manifest
	if err := manifest.WriteTo(table.OutManifestPath); err != nil {
		return err
	}

	// Log statistics
	msg := fmt.Sprintf(
		"Table \"%s\" sliced: in/out: %d / %d slices, %s / %s bytes, %s rows",
		table.Name,
		reader.Slices(), writer.Slices(),
		utils.RemoveSpaces(inputSize.HumanReadable()),
		utils.RemoveSpaces(outBytes.HumanReadable()),
		humanize.Comma(int64(writer.AllRows())),
	)

	switch {
	case !manifest.Exists():
		msg += ", manifest created"
	case manifest.Modified():
		msg += ", manifest updated"
	default:
		msg += ", manifest unaffected"
	}
	msg += "."

	logger.Info(msg)
	return nil
}
