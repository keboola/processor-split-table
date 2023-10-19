// Package processor provides split processor implementation.
package processor

import (
	"fmt"
	"path/filepath"

	"github.com/dustin/go-humanize"

	"github.com/keboola/processor-split-table/internal/pkg/kbc"
	"github.com/keboola/processor-split-table/internal/pkg/log"
	"github.com/keboola/processor-split-table/internal/pkg/processor/config"
	"github.com/keboola/processor-split-table/internal/pkg/processor/finder"
	"github.com/keboola/processor-split-table/internal/pkg/slicer"
	slicerConfig "github.com/keboola/processor-split-table/internal/pkg/slicer/config"
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

	// Find file nodes
	nodes, err := finder.FindFilesRecursive(inputDir)
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
	case slicerConfig.ModeBytes:
		logger.Infof("Configured max %s per slice.", humanize.IBytes(cfg.Parameters.BytesPerSlice))
	case slicerConfig.ModeRows:
		logger.Infof("Configured max %s rows per slice.", humanize.Comma(int64(cfg.Parameters.RowsPerSlice)))
	case slicerConfig.ModeSlices:
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

	// Process found nodes
	for _, node := range nodes {
		switch node.FileType {
		case finder.CsvTableSingle:
			// Single file CSV tables -> split
			if err := slicer.SliceTable(logger, tableDefinition(cfg, node, inputDir, outputDir)); err != nil {
				return err
			}
		case finder.Directory:
			if err := utils.Mkdir(filepath.Join(outputDir, node.RelativePath)); err != nil {
				return err
			}
		case finder.CsvTableSliced:
			// Already sliced tables are copied from in -> out
			table := tableDefinition(cfg, node, inputDir, outputDir)
			logger.Infof("Copying already sliced table \"%s\".", table.Name)
			if err := utils.CopyRecursive(table.InPath, table.OutPath); err != nil {
				return err
			}
			if found, err := utils.FileExists(table.InManifestPath); err != nil {
				return err
			} else if found {
				if err := utils.CopyRecursive(table.InManifestPath, table.OutManifestPath); err != nil {
					return err
				}
			}

		case finder.File:
			// Files are copied from in -> out
			logger.Infof("Copying \"%s\".", node.RelativePath)
			err := utils.CopyRecursive(
				filepath.Join(inputDir, node.RelativePath),
				filepath.Join(outputDir, node.RelativePath),
			)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected FileType \"%v\"", node.FileType)
		}
	}

	return nil
}

func tableDefinition(cfg *config.Config, file *finder.FileNode, inputDir, outputDir string) slicer.Table {
	return slicer.Table{
		Config:          cfg.Parameters,
		Name:            file.RelativePath,
		InPath:          filepath.Join(inputDir, file.RelativePath),
		InManifestPath:  filepath.Join(inputDir, file.ManifestPath),
		OutPath:         filepath.Join(outputDir, file.RelativePath),
		OutManifestPath: filepath.Join(outputDir, file.ManifestPath),
	}
}
