// Package processor provides split processor implementation.
package processor

import (
	"fmt"

	"github.com/dustin/go-humanize"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/log"
	"github.com/keboola/processor-split-table/internal/pkg/processor/config"
	"github.com/keboola/processor-split-table/internal/pkg/processor/finder"
	"github.com/keboola/processor-split-table/internal/pkg/slicer"
	"github.com/keboola/processor-split-table/internal/pkg/utils"
)

func Run(logger log.Logger) error {
	inputDir := kbc.GetInputDir()
	outputDir := kbc.GetOutputDir()

	// Load config
	cfg, err := config.LoadConfig(kbc.GetDataDir() + "/config.json")
	if err != nil {
		return err
	}

	// Find files
	files, err := finder.FindFilesRecursive(inputDir)
	if err != nil {
		return err
	}

	// Create in/out dirs if not exits
	if err := utils.Mkdir(inputDir); err != nil {
		return err
	}
	if err := utils.Mkdir(outputDir); err != nil {
		return err
	}

	// Log settings
	switch cfg.Parameters.Mode {
	case config.ModeBytes:
		logger.Infof("Configured max %s per slice.", humanize.IBytes(cfg.Parameters.BytesPerSlice))
	case config.ModeRows:
		logger.Infof("Configured max %s rows per slice.", humanize.Comma(int64(cfg.Parameters.RowsPerSlice)))
	case config.ModeSlices:
		logger.Infof(
			"Configured number of slices is %d, min %s per slice.",
			cfg.Parameters.NumberOfSlices,
			humanize.IBytes(cfg.Parameters.MinBytesPerSlice),
		)
	default:
		return kbc.UserErrorf("unexpected mode \"%s\".", cfg.Parameters.Mode)
	}

	if cfg.Parameters.Gzip {
		logger.Infof("Gzip enabled, compression level = %d.", cfg.Parameters.GzipLevel)
	}

	// Process all found files
	for _, file := range files {
		inPath := inputDir + "/" + file.RelativePath
		outPath := outputDir + "/" + file.RelativePath
		inManifestPath := inputDir + "/" + file.ManifestPath
		outManifestPath := outputDir + "/" + file.ManifestPath

		switch file.FileType {
		case finder.CsvTableSingle:
			// Single file CSV tables -> split
			if err := slicer.SliceCsv(logger, cfg, file.RelativePath, inPath, inManifestPath, outPath, outManifestPath); err != nil {
				return err
			}
		case finder.Directory:
			if err := utils.Mkdir(outPath); err != nil {
				return err
			}
		case finder.CsvTableSliced:
			// Already sliced tables are copied from in -> out
			logger.Infof("Copying already sliced table \"%s\".", file.RelativePath)
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
			logger.Infof("Copying \"%s\".", file.RelativePath)
			if err := utils.CopyRecursive(inPath, outPath); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected FileType \"%v\"", file.FileType)
		}
	}

	return nil
}
