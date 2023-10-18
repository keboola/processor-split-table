package processor

import (
	"log"

	humanize "github.com/dustin/go-humanize"

	"github.com/keboola/processor-split-table/internal/pkg/config"
	"github.com/keboola/processor-split-table/internal/pkg/csv"
	"github.com/keboola/processor-split-table/internal/pkg/finder"
	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

// Processor processes files found by Finder.
type Processor struct {
	logger    *log.Logger
	config    *config.Config
	inputDir  string
	outputDir string
	files     []*finder.FileNode
}

func NewProcessor(logger *log.Logger, conf *config.Config, inputDir string, outputDir string, files []*finder.FileNode) *Processor {
	return &Processor{logger: logger, config: conf, inputDir: inputDir, outputDir: outputDir, files: files}
}

func (p *Processor) Run() {
	// Create in/out dirs if not exits
	utils.Mkdir(p.inputDir)
	utils.Mkdir(p.outputDir)

	// Log settings
	switch p.config.Parameters.Mode {
	case config.ModeBytes:
		p.logger.Printf("Configured max %s per slice.", humanize.IBytes(p.config.Parameters.BytesPerSlice))
	case config.ModeRows:
		p.logger.Printf("Configured max %s rows per slice.", humanize.Comma(int64(p.config.Parameters.RowsPerSlice)))
	case config.ModeSlices:
		p.logger.Printf(
			"Configured number of slices is %d, min %s per slice.",
			p.config.Parameters.NumberOfSlices,
			humanize.IBytes(p.config.Parameters.MinBytesPerSlice),
		)
	default:
		kbc.PanicApplicationErrorf("Unexpected mode \"%s\".", p.config.Parameters.Mode)
	}

	if p.config.Parameters.Gzip {
		p.logger.Printf("Gzip enabled, compression level = %d.", p.config.Parameters.GzipLevel)
	}

	// Process all found files
	for _, file := range p.files {
		inPath := p.inputDir + "/" + file.RelativePath
		outPath := p.outputDir + "/" + file.RelativePath
		inManifestPath := p.inputDir + "/" + file.ManifestPath
		outManifestPath := p.outputDir + "/" + file.ManifestPath

		switch file.FileType {
		case finder.CsvTableSingle:
			// Single file CSV tables -> split
			csv.SliceCsv(p.logger, p.config, file.RelativePath, inPath, inManifestPath, outPath, outManifestPath)
		case finder.Directory:
			utils.Mkdir(outPath)
		case finder.CsvTableSliced:
			// Already sliced tables are copied from in -> out
			p.logger.Printf("Copying already sliced table \"%s\".\n", file.RelativePath)
			utils.CopyRecursive(inPath, outPath)
			if utils.FileExists(inManifestPath) {
				utils.CopyRecursive(inManifestPath, outManifestPath)
			}

		case finder.File:
			// Files are copied from in -> out
			p.logger.Printf("Copying \"%s\".\n", file.RelativePath)
			utils.CopyRecursive(inPath, outPath)

		default:
			kbc.PanicApplicationErrorf("Unexpected FileType \"%s\".", file.FileType)
		}
	}
}
