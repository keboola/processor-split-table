// Package slicer provider slicing of an input table to an output table according to the configuration.
package slicer

import (
	"fmt"

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
	OutManifestPath string
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

	// Get file size
	fileSize, err := utils.FileSize(table.InPath)
	if err != nil {
		return err
	}

	// Create writer
	writer, err := slicedwriter.New(table.Config, fileSize, table.OutPath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	manifest, err := manifestPkg.LoadManifest(table.InManifestPath)
	if err != nil {
		return err
	}

	// Create reader
	reader, err := rowsreader.NewCsvReader(table.InPath, manifest.Delimiter(), manifest.Enclosure())
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

	// Check if no error
	if reader.Err() != nil {
		return fmt.Errorf("error when reading CSV \"%s\": %w", table.InPath, reader.Err())
	}

	// Write manifest
	if table.OutManifestPath != "" {
		if err := manifest.WriteTo(table.OutManifestPath); err != nil {
			return err
		}
	}

	// Log info
	msg := fmt.Sprintf(
		"Table \"%s\" sliced, written %d slices, %s rows, total size %s",
		table.Name,
		writer.Slices(),
		humanize.Comma(int64(writer.AllRows())),
		humanize.IBytes(writer.AlLBytes()),
	)

	if writer.GzipEnabled() {
		if dirSize, err := utils.DirSize(table.OutPath); err == nil {
			msg += fmt.Sprintf(", gzipped size %s", humanize.IBytes(dirSize))
		} else {
			return err
		}
	}

	switch {
	case !manifest.Exists():
		msg += ", manifest created."
	case manifest.Modified():
		msg += ", columns added to manifest."
	default:
		msg += "."
	}

	logger.Info(msg)
	return nil
}
