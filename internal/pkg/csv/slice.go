package csv

import (
	"fmt"
	"log"

	humanize "github.com/dustin/go-humanize"

	"github.com/keboola/processor-split-table/internal/pkg/config"
	manifestPkg "github.com/keboola/processor-split-table/internal/pkg/csv/manifest"
	"github.com/keboola/processor-split-table/internal/pkg/csv/rowsreader"
	"github.com/keboola/processor-split-table/internal/pkg/csv/slicedwriter"
	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

func SliceCsv(logger *log.Logger, conf *config.Config, relativePath string, inPath string, inManifestPath string, outPath string, outManifestPath string) {
	logger.Printf("Slicing table \"%s\".\n", relativePath)

	// Create target dir
	utils.Mkdir(outPath)

	// Create writer
	writer := slicedwriter.NewSlicedWriterFromConf(conf, utils.FileSize(inPath), outPath)
	defer writer.Close()

	// Load manifest, may not exist
	createManifest := !utils.FileExists(inManifestPath)
	manifest := manifestPkg.LoadManifest(inManifestPath)

	// Create reader
	reader := rowsreader.NewCsvReader(inPath, manifest.GetDelimiter(), manifest.GetEnclosure())

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
		kbc.PanicApplicationErrorf("Error when reading CSV \"%s\": %s", inPath, reader.Err())
	}

	// Write manifest
	manifest.WriteTo(outManifestPath)

	// Log info
	logResult(logger, writer, relativePath, outPath, createManifest, addColumnsToManifest)
}

func logResult(logger *log.Logger, w *slicedwriter.SlicedWriter, relativePath string, absPath string, createManifest bool, addColumnsToManifest bool) {
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
