package fecschedulealayout

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

type parquetSchemaMapping struct {
	schema          *parquet.Schema
	parquetToSource []int
	sourceToParquet [schedulea.FieldCount]int
	columns         [schedulea.FieldCount]schedulea.Column
}

func newParquetSchema(selected []string) (parquetSchemaMapping, error) {
	columns := schedulea.Columns()
	selectedSet := make(map[string]struct{}, len(selected))
	if len(selected) > 0 {
		for _, name := range selected {
			selectedSet[name] = struct{}{}
		}
	}
	group := make(parquet.Group, len(columns))
	for _, column := range columns {
		if len(selectedSet) > 0 {
			if _, ok := selectedSet[column.Name]; !ok {
				continue
			}
		}
		var node parquet.Node = parquet.String()
		if column.Kind == schedulea.KindBoolean {
			node = parquet.Leaf(parquet.BooleanType)
		}
		if column.Nullable {
			node = parquet.Optional(node)
		}
		group[column.Name] = node
	}
	if len(group) != len(selectedSet) && len(selectedSet) > 0 {
		return parquetSchemaMapping{}, errors.New("projection contains an unknown Schedule A column")
	}
	schema := parquet.NewSchema("fec_schedule_a", group)
	mapping := parquetSchemaMapping{schema: schema, parquetToSource: make([]int, len(schema.Columns())), columns: columns}
	for parquetIndex, path := range schema.Columns() {
		if len(path) != 1 {
			return parquetSchemaMapping{}, fmt.Errorf("unexpected nested Parquet path %v", path)
		}
		sourceIndex, ok := schedulea.ColumnIndex(path[0])
		if !ok {
			return parquetSchemaMapping{}, fmt.Errorf("Parquet column %s is absent from Schedule A", path[0])
		}
		mapping.parquetToSource[parquetIndex] = sourceIndex
		mapping.sourceToParquet[sourceIndex] = parquetIndex
	}
	return mapping, nil
}

func writeParquet(ctx context.Context, options Options) (ParquetMeasurement, error) {
	var result ParquetMeasurement
	mapping, err := newParquetSchema(nil)
	if err != nil {
		return result, err
	}
	directory := filepath.Join(options.OutputDirectory, "parquet")
	if err := os.Mkdir(directory, 0o750); err != nil {
		return result, err
	}
	started := options.Clock()
	semantic := newSemanticHasher()
	var current *parquetShardWriter
	var totalRows uint64
	_, rows, scanErr := scanSource(ctx, options.SourcePath, options.Cycle, options.Rows, func(row *schedulea.Row) error {
		if current == nil || current.rows == options.RowsPerFile {
			if current != nil {
				file, err := current.Close()
				if err != nil {
					return err
				}
				result.Files = append(result.Files, file)
				result.RowGroups += file.RowGroups
			}
			current, err = newParquetShardWriter(directory, len(result.Files), row.Number(), options, &mapping)
			if err != nil {
				return err
			}
		}
		if err := current.Write(row, &mapping); err != nil {
			return err
		}
		if err := semantic.AddScheduleARow(row); err != nil {
			return err
		}
		totalRows++
		return nil
	})
	if current != nil {
		file, closeErr := current.Close()
		if closeErr != nil && scanErr == nil {
			scanErr = closeErr
		}
		if closeErr == nil {
			result.Files = append(result.Files, file)
			result.RowGroups += file.RowGroups
		}
	}
	if scanErr != nil {
		return result, scanErr
	}
	if rows != options.Rows || totalRows != options.Rows {
		return result, fmt.Errorf("Parquet writer read %d rows and wrote %d; want %d", rows, totalRows, options.Rows)
	}
	var totalBytes int64
	for _, file := range result.Files {
		totalBytes += file.Bytes
	}
	result.LayoutMeasurement = layoutMeasurement(rows, totalBytes, options.Clock().Sub(started))
	result.SemanticSHA256 = semantic.Sum()
	return result, nil
}

type parquetShardWriter struct {
	path     string
	firstRow uint64
	rows     uint64
	output   *os.File
	writer   *parquet.Writer
	digest   hash.Hash
	row      parquet.Row
}

func newParquetShardWriter(directory string, index int, firstRow uint64, options Options, mapping *parquetSchemaMapping) (*parquetShardWriter, error) {
	path := filepath.Join(directory, fmt.Sprintf("part-%05d.parquet", index))
	output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, err
	}
	digest := sha256.New()
	counted := &countingWriter{writer: io.MultiWriter(output, digest)}
	writer := parquet.NewWriter(
		counted,
		mapping.schema,
		parquet.Compression(&parquetzstd.Codec{Level: parquetzstd.SpeedDefault, Concurrency: 1}),
		parquet.MaxRowsPerRowGroup(int64(options.RowsPerRowGroup)),
		parquet.PageBufferSize(256<<10),
		parquet.WriteBufferSize(256<<10),
		parquet.DataPageStatistics(true),
		parquet.CreatedBy("legal-tender", SchemaVersion, "benchmark"),
	)
	return &parquetShardWriter{
		path: path, firstRow: firstRow, output: output, writer: writer,
		digest: digest,
		row:    make(parquet.Row, len(mapping.parquetToSource)),
	}, nil
}

func (w *parquetShardWriter) Write(source *schedulea.Row, mapping *parquetSchemaMapping) error {
	for parquetIndex, sourceIndex := range mapping.parquetToSource {
		field, ok := source.Field(sourceIndex)
		if !ok {
			return fmt.Errorf("source row %d is missing field %d", source.Number(), sourceIndex)
		}
		definitionLevel := 0
		if mapping.columns[sourceIndex].Nullable && !field.IsNull() {
			definitionLevel = 1
		}
		if field.IsNull() {
			w.row[parquetIndex] = parquet.NullValue().Level(0, 0, parquetIndex)
		} else if mapping.columns[sourceIndex].Kind == schedulea.KindBoolean {
			w.row[parquetIndex] = parquet.BooleanValue(len(field.Bytes()) == 1 && field.Bytes()[0] == 't').Level(0, definitionLevel, parquetIndex)
		} else {
			w.row[parquetIndex] = parquet.ByteArrayValue(field.Bytes()).Level(0, definitionLevel, parquetIndex)
		}
	}
	if _, err := w.writer.WriteRows([]parquet.Row{w.row}); err != nil {
		return err
	}
	w.rows++
	return nil
}

func (w *parquetShardWriter) Close() (ParquetFile, error) {
	var result ParquetFile
	if err := w.writer.Close(); err != nil {
		_ = w.output.Close()
		return result, err
	}
	if err := w.output.Close(); err != nil {
		return result, err
	}
	file, err := os.Open(w.path)
	if err != nil {
		return result, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return result, err
	}
	opened, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		_ = file.Close()
		return result, err
	}
	rowGroups := uint64(len(opened.RowGroups()))
	if err := file.Close(); err != nil {
		return result, err
	}
	result = ParquetFile{
		Path: filepath.Base(w.path), FirstRowOrdinal: w.firstRow,
		LastRowOrdinal: w.firstRow + w.rows - 1, Rows: w.rows,
		RowGroups: rowGroups, Bytes: info.Size(), SHA256: hex.EncodeToString(w.digest.Sum(nil)),
	}
	return result, nil
}

func scanParquetFull(ctx context.Context, directory string, files []ParquetFile) (string, uint64, error) {
	mapping, err := newParquetSchema(nil)
	if err != nil {
		return "", 0, err
	}
	semantic := newSemanticHasher()
	var rows uint64
	for _, candidate := range files {
		path := filepath.Join(directory, "parquet", candidate.Path)
		file, reader, err := openParquetReader(path, nil)
		if err != nil {
			return "", rows, err
		}
		buffer := make([]parquet.Row, 256)
		for {
			n, readErr := reader.ReadRows(buffer)
			for _, row := range buffer[:n] {
				if err := addParquetSemanticRow(semantic, row, &mapping); err != nil {
					_ = reader.Close()
					_ = file.Close()
					return "", rows, err
				}
				rows++
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				_ = reader.Close()
				_ = file.Close()
				return "", rows, readErr
			}
			if rows&0x3fff == 0 {
				if err := ctx.Err(); err != nil {
					_ = reader.Close()
					_ = file.Close()
					return "", rows, err
				}
			}
		}
		if err := reader.Close(); err != nil {
			_ = file.Close()
			return "", rows, err
		}
		if err := file.Close(); err != nil {
			return "", rows, err
		}
	}
	return semantic.Sum(), rows, nil
}

func addParquetSemanticRow(semantic *semanticHasher, row parquet.Row, mapping *parquetSchemaMapping) error {
	if len(row) != len(mapping.parquetToSource) {
		return fmt.Errorf("Parquet row has %d values; want %d", len(row), len(mapping.parquetToSource))
	}
	var values [schedulea.FieldCount]logicalValue
	for parquetIndex, value := range row {
		sourceIndex := mapping.parquetToSource[parquetIndex]
		values[sourceIndex] = logicalValueFromParquet(value, mapping.columns[sourceIndex].Kind)
	}
	for _, value := range values {
		semantic.add(value.null, value.bytes)
	}
	return nil
}

func scanParquetProjection(ctx context.Context, directory string, files []ParquetFile) (ScanMeasurement, error) {
	started := time.Now()
	fullMapping, err := newParquetSchema(nil)
	if err != nil {
		return ScanMeasurement{}, err
	}
	indexes, err := projectionIndexes()
	if err != nil {
		return ScanMeasurement{}, err
	}
	projectionHash := newProjectionHasher()
	var decisions decisionAccumulator
	var rows uint64
	for _, candidate := range files {
		path := filepath.Join(directory, "parquet", candidate.Path)
		file, err := os.Open(path)
		if err != nil {
			return ScanMeasurement{}, err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return ScanMeasurement{}, err
		}
		opened, err := parquet.OpenFile(file, info.Size())
		if err != nil {
			_ = file.Close()
			return ScanMeasurement{}, err
		}
		for _, rowGroup := range opened.RowGroups() {
			if err := scanProjectionRowGroup(ctx, rowGroup, &fullMapping, indexes, projectionHash, &decisions, &rows); err != nil {
				_ = file.Close()
				return ScanMeasurement{}, err
			}
		}
		if err := file.Close(); err != nil {
			return ScanMeasurement{}, err
		}
	}
	return scanMeasurement(rows, time.Since(started), projectionHash.Sum(), decisions.Result()), nil
}

func scanProjectionRowGroup(ctx context.Context, rowGroup parquet.RowGroup, mapping *parquetSchemaMapping, indexes projectionIndexSet, projectionHash *projectionHasher, decisions *decisionAccumulator, rows *uint64) error {
	allChunks := rowGroup.ColumnChunks()
	chunks := make([]parquet.ColumnChunk, lenProjectionFields)
	for projectionIndex, sourceIndex := range indexes {
		parquetIndex := mapping.sourceToParquet[sourceIndex]
		if parquetIndex >= len(allChunks) {
			return fmt.Errorf("Parquet row group is missing column %s", projectionFieldNames[projectionIndex])
		}
		chunks[projectionIndex] = allChunks[parquetIndex]
	}
	reader := parquet.NewColumnChunkRowReader(chunks)
	buffer := make([]parquet.Row, 4096)
	for {
		n, readErr := reader.ReadRows(buffer)
		for _, row := range buffer[:n] {
			if len(row) != lenProjectionFields {
				_ = reader.Close()
				return fmt.Errorf("projected Parquet row has %d values; want %d", len(row), lenProjectionFields)
			}
			var values projectionValues
			for projectionIndex, sourceIndex := range indexes {
				values[projectionIndex] = logicalValueFromParquet(row[projectionIndex], mapping.columns[sourceIndex].Kind)
			}
			projectionHash.Add(values)
			if err := decisions.Add(values); err != nil {
				_ = reader.Close()
				return err
			}
			*rows++
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			_ = reader.Close()
			return readErr
		}
		if err := ctx.Err(); err != nil {
			_ = reader.Close()
			return err
		}
	}
	return reader.Close()
}

func logicalValueFromParquet(value parquet.Value, kind schedulea.Kind) logicalValue {
	if value.IsNull() {
		return logicalValue{null: true}
	}
	if kind == schedulea.KindBoolean {
		if value.Boolean() {
			return logicalValue{bytes: trueLexeme}
		}
		return logicalValue{bytes: falseLexeme}
	}
	return logicalValue{bytes: value.ByteArray()}
}

var (
	trueLexeme  = []byte{'t'}
	falseLexeme = []byte{'f'}
)

func openParquetReader(path string, schema *parquet.Schema) (*os.File, *parquet.Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	var reader *parquet.Reader
	if schema == nil {
		reader = parquet.NewReader(file)
	} else {
		reader = parquet.NewReader(file, schema)
	}
	return file, reader, nil
}
