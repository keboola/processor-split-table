package csv

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"keboola.processor-split-by-nrows/src/config"
	manifestPkg "keboola.processor-split-by-nrows/src/csv/manifest"
	"keboola.processor-split-by-nrows/src/csv/rowsReader"
	"keboola.processor-split-by-nrows/src/csv/slicedWriter"
	"keboola.processor-split-by-nrows/src/kbc"
	"keboola.processor-split-by-nrows/src/utils"
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
	reader := rowsReader.NewCsvReader(inPath)

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
	logResult(logger, writer, relativePath, createManifest, addColumnsToManifest)

}

func logResult(logger *log.Logger, writer *slicedWriter.SlicedWriter, relativePath string, createManifest bool, addColumnsToManifest bool) {
	msg := fmt.Sprintf(
		"Table \"%s\" sliced. Written %d parts, %s rows, total size %s.",
		relativePath,
		writer.Slices(),
		humanize.Comma(int64(writer.AllRows())),
		humanize.IBytes(writer.AlLBytes()),
	)
	if createManifest {
		msg += " Manifest created."
	} else if addColumnsToManifest {
		msg += " Columns added to manifest."
	}
	logger.Println(msg)
}
