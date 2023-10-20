// Package slicer provider slicing of an input table to an output table according to the configuration.
package slicer

import (
	"fmt"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/go-playground/validator/v10"

	"github.com/keboola/processor-split-table/internal/pkg/log"
	manifestPkg "github.com/keboola/processor-split-table/internal/pkg/manifest"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/config"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/rowsreader"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/slicedwriter"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

type Table struct {
	config.Config
	Name            string `validate:"required"`
	InPath          string `validate:"required"`
	InManifestPath  string
	OutPath         string `validate:"required"`
	OutManifestPath string `validate:"required"`
}

func SliceTable(logger log.Logger, table Table) (err error) {
	logger.Infof("Slicing table \"%s\".", table.Name)

	// Validate
	val := validator.New()
	if err := val.Struct(table); err != nil {
		return fmt.Errorf(`table definition is not valid: %w`, err)
	}

	// Create target dir
	if err := utils.Mkdir(table.OutPath); err != nil {
		return err
	}

	// Get input type
	stat, err := os.Stat(table.InPath)
	if err != nil {
		return err
	}
	slicedInput := stat.IsDir()

	// Get input size
	var inputSize int64
	if slicedInput {
		inputSize, err = utils.DirSize(table.InPath)
		if err != nil {
			return err
		}
	} else {
		inputSize = stat.Size()
	}

	// Load manifest
	manifest, err := manifestPkg.LoadManifest(table.InManifestPath)
	if err != nil {
		return err
	}

	// Create writer
	writer, err := slicedwriter.New(table.Config, inputSize, table.OutPath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	// Create reader
	var reader *rowsreader.CSVReader
	if slicedInput {
		reader, err = rowsreader.NewSlicesReader(table.InPath, manifest.Delimiter(), manifest.Enclosure())
	} else {
		reader, err = rowsreader.NewFileReader(table.InPath, manifest.Delimiter(), manifest.Enclosure())
	}
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

	// Write manifest
	if err := manifest.WriteTo(table.OutManifestPath); err != nil {
		return err
	}

	// Get output size
	outBytes := writer.AlLBytes()
	if writer.GzipEnabled() {
		if dirSize, err := utils.DirSize(table.OutPath); err == nil {
			outBytes = uint64(dirSize)
		} else {
			return err
		}
	}

	// Log statistics
	msg := fmt.Sprintf(
		"Table \"%s\" sliced: in/out: %d / %d slices, %s / %s bytes, %s rows",
		table.Name,
		reader.Slices(), writer.Slices(),
		strings.ReplaceAll(humanize.IBytes(uint64(inputSize)), " ", ""),
		strings.ReplaceAll(humanize.IBytes(outBytes), " ", ""),
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
