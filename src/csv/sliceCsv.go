package csv

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"keboola.processor-split-table/src/config"
	manifestPkg "keboola.processor-split-table/src/csv/manifest"
	"keboola.processor-split-table/src/csv/rowsReader"
	"keboola.processor-split-table/src/csv/slicedWriter"
	"keboola.processor-split-table/src/kbc"
	"keboola.processor-split-table/src/utils"
	"log"
)

func SliceCsv(logger *log.Logger, conf *config.Config, relativePath string, inPath string, inManifestPath string, outPath string, outManifestPath string) {
	logger.Printf("Slicing table \"%s\".\n", relativePath)

	// Create target dir
	utils.Mkdir(outPath)

	// Create writer
	writer := slicedWriter.NewSlicedWriter(conf, outPath)
	defer writer.Close()

	// Create reader
	reader := rowsReader.NewCsvReader(inPath, ',', '"')

	// Load manifest, may not exist
	createManifest := !utils.FileExists(inManifestPath)
	manifest := manifestPkg.LoadManifest(inManifestPath)

	// If manifest without defined columns -> store first row/header to manifest "columns" key
	addColumnsToManifest := !manifest.HasColumns()
	if addColumnsToManifest {
		manifest.SetColumns(reader.Header())
	}

	// Read all rows from input table and write to sliced table
	for reader.Read() {
		writer.Write(reader.Bytes())
	}

	// Check if no error
	if reader.Err() != nil {
		kbc.PanicApplicationError("Error when reading CSV \"%s\": %s", inPath, reader.Err())
	}

	// Write manifest
	manifest.WriteTo(outManifestPath)

	// Log info
	logResult(logger, writer, relativePath, outPath, createManifest, addColumnsToManifest)

}

func logResult(logger *log.Logger, w *slicedWriter.SlicedWriter, relativePath string, absPath string, createManifest bool, addColumnsToManifest bool) {
	msg := fmt.Sprintf(
		"Table \"%s\" sliced. Written %d parts, %s rows, total size %s.",
		relativePath,
		w.Slices(),
		humanize.Comma(int64(w.AllRows())),
		humanize.IBytes(w.AlLBytes()),
	)

	if w.GzipEnabled() {
		msg += fmt.Sprintf(" Gzipped size %s.", humanize.IBytes(utils.DirSize(absPath)))
	}

	if createManifest {
		msg += " Manifest created."
	} else if addColumnsToManifest {
		msg += " Columns added to manifest."
	}

	logger.Println(msg)
}
