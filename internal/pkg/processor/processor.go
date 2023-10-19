package processor

import (
	"fmt"

	"github.com/dustin/go-humanize"

	"github.com/keboola/processor-split-table/internal/pkg/finder"
	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/log"
	"github.com/keboola/processor-split-table/internal/pkg/processor/config"
	"github.com/keboola/processor-split-table/internal/pkg/slicer"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

// Processor processes files found by Finder.
type Processor struct {
	logger    log.Logger
	config    *config.Config
	inputDir  string
	outputDir string
	files     []*finder.FileNode
}

func NewProcessor(logger log.Logger, conf *config.Config, inputDir string, outputDir string, files []*finder.FileNode) *Processor {
	return &Processor{logger: logger, config: conf, inputDir: inputDir, outputDir: outputDir, files: files}
}

func (p *Processor) Run() error {
	// Create in/out dirs if not exits
	if err := utils.Mkdir(p.inputDir); err != nil {
		return err
	}
	if err := utils.Mkdir(p.outputDir); err != nil {
		return err
	}

	// Log settings
	switch p.config.Parameters.Mode {
	case config.ModeBytes:
		p.logger.Infof("Configured max %s per slice.", humanize.IBytes(p.config.Parameters.BytesPerSlice))
	case config.ModeRows:
		p.logger.Infof("Configured max %s rows per slice.", humanize.Comma(int64(p.config.Parameters.RowsPerSlice)))
	case config.ModeSlices:
		p.logger.Infof(
			"Configured number of slices is %d, min %s per slice.",
			p.config.Parameters.NumberOfSlices,
			humanize.IBytes(p.config.Parameters.MinBytesPerSlice),
		)
	default:
		return kbc.UserErrorf("unexpected mode \"%s\".", p.config.Parameters.Mode)
	}

	if p.config.Parameters.Gzip {
		p.logger.Infof("Gzip enabled, compression level = %d.", p.config.Parameters.GzipLevel)
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
			if err := slicer.SliceCsv(p.logger, p.config, file.RelativePath, inPath, inManifestPath, outPath, outManifestPath); err != nil {
				return err
			}
		case finder.Directory:
			if err := utils.Mkdir(outPath); err != nil {
				return err
			}
		case finder.CsvTableSliced:
			// Already sliced tables are copied from in -> out
			p.logger.Infof("Copying already sliced table \"%s\".", file.RelativePath)
			if err := utils.CopyRecursive(inPath, outPath); err != nil {
				return err
			}
			if found, err := utils.FileExists(inManifestPath); err != nil {
				return err
			} else if found {
				if err := utils.CopyRecursive(inManifestPath, outManifestPath); err != nil {
					return err
				}
			}

		case finder.File:
			// Files are copied from in -> out
			p.logger.Infof("Copying \"%s\".", file.RelativePath)
			if err := utils.CopyRecursive(inPath, outPath); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected FileType \"%v\"", file.FileType)
		}
	}

	return nil
}
