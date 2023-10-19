package slicer

import (
	"fmt"

	"github.com/dustin/go-humanize"

	"github.com/keboola/processor-split-table/internal/pkg/config"
	"github.com/keboola/processor-split-table/internal/pkg/log"
	manifestPkg "github.com/keboola/processor-split-table/internal/pkg/slicer/manifest"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/rowsreader"
	"github.com/keboola/processor-split-table/internal/pkg/slicer/slicedwriter"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

func SliceCsv(logger log.Logger, conf *config.Config, relativePath string, inPath string, inManifestPath string, outPath string, outManifestPath string) (err error) {
	logger.Infof("Slicing table \"%s\".", relativePath)

	// Create target dir
	if err := utils.Mkdir(outPath); err != nil {
		return err
	}

	// Get file size
	fileSize, err := utils.FileSize(inPath)
	if err != nil {
		return err
	}

	// Create writer
	writer, err := slicedwriter.NewSlicedWriterFromConf(conf, fileSize, outPath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	// Load manifest, may not exist
	found, err := utils.FileExists(inManifestPath)
	if err != nil {
		return err
	}

	createManifest := !found
	manifest, err := manifestPkg.LoadManifest(inManifestPath)
	if err != nil {
		return err
	}

	// Create reader
	reader, err := rowsreader.NewCsvReader(inPath, manifest.Delimiter(), manifest.Enclosure())
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
		return fmt.Errorf("error when reading CSV \"%s\": %w", inPath, reader.Err())
	}

	// Write manifest
	if err := manifest.WriteTo(outManifestPath); err != nil {
		return err
	}

	// Log info
	return logResult(logger, writer, relativePath, outPath, createManifest, addColumnsToManifest)
}

func logResult(logger log.Logger, w *slicedwriter.SlicedWriter, relativePath string, absPath string, createManifest bool, addColumnsToManifest bool) error {
	msg := fmt.Sprintf(
		"Table \"%s\" sliced, written %d slices, %s rows, total size %s",
		relativePath,
		w.Slices(),
		humanize.Comma(int64(w.AllRows())),
		humanize.IBytes(w.AlLBytes()),
	)

	if w.GzipEnabled() {
		if dirSize, err := utils.DirSize(absPath); err == nil {
			msg += fmt.Sprintf(", gzipped size %s", humanize.IBytes(dirSize))
		} else {
			return err
		}
	}

	switch {
	case createManifest:
		msg += ", manifest created."
	case addColumnsToManifest:
		msg += ", columns added to manifest."
	default:
		msg += "."
	}

	logger.Info(msg)
	return nil
}
