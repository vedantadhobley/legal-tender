// Package schedulebparquet owns the physical Parquet contract for processed
// Schedule B disbursement facts. It preserves every source value and adds only a
// narrow, policy-free typed projection.
package schedulebparquet

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
)

const (
	// PhysicalSchemaVersion changes whenever the on-disk Parquet columns or
	// their meanings change.
	PhysicalSchemaVersion = "legal-tender.fec.schedule-b-parquet.v1"
	// SemanticDigestVersion binds the lossless source-value and locator digest.
	SemanticDigestVersion = "legal-tender.fec.schedule-b-parquet-semantic.v1"
	// LibraryVersion records the pinned writer used for this implementation.
	LibraryVersion = "github.com/parquet-go/parquet-go@v0.32.0"
)

const (
	ColumnSourceRowOrdinal              = "lt_source_row_ordinal"
	ColumnSourceRawByteOffset           = "lt_source_raw_byte_offset"
	ColumnSourceRawByteLength           = "lt_source_raw_byte_length"
	ColumnDisbursementAmountMinorUnits  = "lt_disbursement_amount_minor_units"
	ColumnDisbursementAmountSourceScale = "lt_disbursement_amount_source_scale"
	ColumnDisbursementAmountState       = "lt_disbursement_amount_state"
	ColumnBundledRefundMinorUnits       = "lt_bundled_refund_minor_units"
	ColumnBundledRefundSourceScale      = "lt_bundled_refund_source_scale"
	ColumnBundledRefundState            = "lt_bundled_refund_state"
	ColumnDisbursementDate              = "lt_disbursement_date"
	ColumnDisbursementAtLocal           = "lt_disbursement_at_local"
	ColumnCommunicationDate             = "lt_communication_date"
	ColumnCommunicationAtLocal          = "lt_communication_at_local"
	ColumnPublisherLoadedAtLocal        = "lt_publisher_loaded_at_local"
	ColumnReportYear                    = "lt_report_year"
	ColumnTwoYearTransactionPeriod      = "lt_two_year_transaction_period"
	ColumnMemoedSubtotal                = "lt_memoed_subtotal"
)

// Metadata is sufficient to resolve a Parquet fact back to the immutable COPY
// source without repeating per-row occurrence and record-version hashes.
type Metadata struct {
	SourceRowOrdinal    uint64
	SourceRawByteOffset uint64
	SourceRawByteLength uint64
}

// Derived contains only lossless or deterministic typed projections. It does
// not decide whether a disbursement counts, select amendments, or resolve
// entities.
type Derived struct {
	DisbursementAmountMinorUnits  *int64
	DisbursementAmountSourceScale *int32
	DisbursementAmountState       string
	BundledRefundMinorUnits       *int64
	BundledRefundSourceScale      *int32
	BundledRefundState            string
	DisbursementDateDays          *int32
	DisbursementAtLocalNanos      *int64
	CommunicationDateDays         *int32
	CommunicationAtLocalNanos     *int64
	PublisherLoadedAtNanos        *int64
	ReportYear                    *int64
	TwoYearTransactionPeriod      int64
	MemoedSubtotal                bool
}

type valueKind uint8

const (
	valueString valueKind = iota
	valueBoolean
	valueInt32
	valueInt64
)

type binding struct {
	name        string
	kind        valueKind
	nullable    bool
	sourceIndex int
}

// Schema binds the stable file schema to source-column ordinals.
type Schema struct {
	parquet         *parquet.Schema
	bindings        []binding
	columns         [scheduleb.FieldCount]scheduleb.Column
	sourceToParquet [scheduleb.FieldCount]int
	nameToParquet   map[string]int
}

// NewSchema constructs the complete flat physical schema. Source columns keep
// their publisher names; Legal Tender metadata uses the reserved lt_ prefix.
func NewSchema() (*Schema, error) {
	columns := scheduleb.Columns()
	group := make(parquet.Group, scheduleb.FieldCount+17)
	bindingsByName := make(map[string]binding, scheduleb.FieldCount+17)
	add := func(candidate binding, node parquet.Node) error {
		if _, exists := group[candidate.name]; exists {
			return fmt.Errorf("duplicate Schedule B Parquet column %s", candidate.name)
		}
		if candidate.nullable {
			node = parquet.Optional(node)
		}
		group[candidate.name] = node
		bindingsByName[candidate.name] = candidate
		return nil
	}

	for index, column := range columns {
		kind := valueString
		var node parquet.Node = parquet.String()
		if err := add(binding{name: column.Name, kind: kind, nullable: column.Nullable, sourceIndex: index}, node); err != nil {
			return nil, err
		}
	}

	metadata := []struct {
		binding binding
		node    parquet.Node
	}{
		{binding{name: ColumnSourceRowOrdinal, kind: valueInt64, sourceIndex: -1}, parquet.Leaf(parquet.Int64Type)},
		{binding{name: ColumnSourceRawByteOffset, kind: valueInt64, sourceIndex: -1}, parquet.Leaf(parquet.Int64Type)},
		{binding{name: ColumnSourceRawByteLength, kind: valueInt64, sourceIndex: -1}, parquet.Leaf(parquet.Int64Type)},
		{binding{name: ColumnDisbursementAmountMinorUnits, kind: valueInt64, nullable: true, sourceIndex: -1}, parquet.Leaf(parquet.Int64Type)},
		{binding{name: ColumnDisbursementAmountSourceScale, kind: valueInt32, nullable: true, sourceIndex: -1}, parquet.Leaf(parquet.Int32Type)},
		{binding{name: ColumnDisbursementAmountState, kind: valueString, sourceIndex: -1}, parquet.String()},
		{binding{name: ColumnBundledRefundMinorUnits, kind: valueInt64, nullable: true, sourceIndex: -1}, parquet.Leaf(parquet.Int64Type)},
		{binding{name: ColumnBundledRefundSourceScale, kind: valueInt32, nullable: true, sourceIndex: -1}, parquet.Leaf(parquet.Int32Type)},
		{binding{name: ColumnBundledRefundState, kind: valueString, sourceIndex: -1}, parquet.String()},
		{binding{name: ColumnDisbursementDate, kind: valueInt32, nullable: true, sourceIndex: -1}, parquet.Date()},
		{binding{name: ColumnDisbursementAtLocal, kind: valueInt64, nullable: true, sourceIndex: -1}, parquet.TimestampAdjusted(parquet.Nanosecond, false)},
		{binding{name: ColumnCommunicationDate, kind: valueInt32, nullable: true, sourceIndex: -1}, parquet.Date()},
		{binding{name: ColumnCommunicationAtLocal, kind: valueInt64, nullable: true, sourceIndex: -1}, parquet.TimestampAdjusted(parquet.Nanosecond, false)},
		{binding{name: ColumnPublisherLoadedAtLocal, kind: valueInt64, nullable: true, sourceIndex: -1}, parquet.TimestampAdjusted(parquet.Nanosecond, false)},
		{binding{name: ColumnReportYear, kind: valueInt64, nullable: true, sourceIndex: -1}, parquet.Leaf(parquet.Int64Type)},
		{binding{name: ColumnTwoYearTransactionPeriod, kind: valueInt64, sourceIndex: -1}, parquet.Leaf(parquet.Int64Type)},
		{binding{name: ColumnMemoedSubtotal, kind: valueBoolean, sourceIndex: -1}, parquet.Leaf(parquet.BooleanType)},
	}
	for _, candidate := range metadata {
		if err := add(candidate.binding, candidate.node); err != nil {
			return nil, err
		}
	}

	parquetSchema := parquet.NewSchema("fec_schedule_b_disbursement", group)
	result := &Schema{
		parquet: parquetSchema, bindings: make([]binding, len(parquetSchema.Columns())), columns: columns,
		nameToParquet: make(map[string]int, len(parquetSchema.Columns())),
	}
	for parquetIndex, path := range parquetSchema.Columns() {
		if len(path) != 1 {
			return nil, fmt.Errorf("unexpected nested Schedule B Parquet path %v", path)
		}
		candidate, ok := bindingsByName[path[0]]
		if !ok {
			return nil, fmt.Errorf("unbound Schedule B Parquet column %s", path[0])
		}
		result.bindings[parquetIndex] = candidate
		result.nameToParquet[candidate.name] = parquetIndex
		if candidate.sourceIndex >= 0 {
			result.sourceToParquet[candidate.sourceIndex] = parquetIndex
		}
	}
	return result, nil
}

// Parquet returns the schema expected by parquet-go readers and writers.
func (schema *Schema) Parquet() *parquet.Schema { return schema.parquet }

// ColumnCount returns the complete physical leaf-column count.
func (schema *Schema) ColumnCount() int { return len(schema.bindings) }

// ColumnIndex resolves one flat physical column name.
func (schema *Schema) ColumnIndex(name string) (int, bool) {
	index, ok := schema.nameToParquet[name]
	return index, ok
}

// Encode maps one validated source row plus its policy-free projection to a
// flat Parquet row. The returned slice may be reused by the next call.
func (schema *Schema) Encode(destination parquet.Row, source *scheduleb.Row, metadata Metadata, derived Derived) (parquet.Row, error) {
	if err := scheduleb.Validate(source, ""); err != nil {
		return nil, err
	}
	if metadata.SourceRowOrdinal != source.Number() || metadata.SourceRowOrdinal == 0 {
		return nil, fmt.Errorf("Schedule B Parquet locator row ordinal is inconsistent")
	}
	if metadata.SourceRowOrdinal > math.MaxInt64 || metadata.SourceRawByteOffset > math.MaxInt64 || metadata.SourceRawByteLength > math.MaxInt64 {
		return nil, fmt.Errorf("Schedule B Parquet locator exceeds signed physical range")
	}
	if derived.DisbursementAmountState == "" || derived.BundledRefundState == "" {
		return nil, fmt.Errorf("Schedule B Parquet typed states are required")
	}
	if len(destination) != len(schema.bindings) {
		destination = make(parquet.Row, len(schema.bindings))
	}
	for parquetIndex, candidate := range schema.bindings {
		value, isNull, err := schema.value(candidate, source, metadata, derived)
		if err != nil {
			return nil, err
		}
		if isNull {
			if !candidate.nullable {
				return nil, fmt.Errorf("required Schedule B Parquet column %s is null", candidate.name)
			}
			destination[parquetIndex] = parquet.NullValue().Level(0, 0, parquetIndex)
			continue
		}
		definitionLevel := 0
		if candidate.nullable {
			definitionLevel = 1
		}
		destination[parquetIndex] = value.Level(0, definitionLevel, parquetIndex)
	}
	return destination, nil
}

func (schema *Schema) value(candidate binding, source *scheduleb.Row, metadata Metadata, derived Derived) (parquet.Value, bool, error) {
	if candidate.sourceIndex >= 0 {
		field, ok := source.Field(candidate.sourceIndex)
		if !ok {
			return parquet.Value{}, false, fmt.Errorf("source row %d is missing field %d", source.Number(), candidate.sourceIndex+1)
		}
		if field.IsNull() {
			return parquet.Value{}, true, nil
		}
		return parquet.ByteArrayValue(field.Bytes()), false, nil
	}

	switch candidate.name {
	case ColumnSourceRowOrdinal:
		return parquet.Int64Value(int64(metadata.SourceRowOrdinal)), false, nil
	case ColumnSourceRawByteOffset:
		return parquet.Int64Value(int64(metadata.SourceRawByteOffset)), false, nil
	case ColumnSourceRawByteLength:
		return parquet.Int64Value(int64(metadata.SourceRawByteLength)), false, nil
	case ColumnDisbursementAmountMinorUnits:
		return optionalInt64Value(derived.DisbursementAmountMinorUnits)
	case ColumnDisbursementAmountSourceScale:
		return optionalInt32Value(derived.DisbursementAmountSourceScale)
	case ColumnDisbursementAmountState:
		return parquet.ByteArrayValue([]byte(derived.DisbursementAmountState)), false, nil
	case ColumnBundledRefundMinorUnits:
		return optionalInt64Value(derived.BundledRefundMinorUnits)
	case ColumnBundledRefundSourceScale:
		return optionalInt32Value(derived.BundledRefundSourceScale)
	case ColumnBundledRefundState:
		return parquet.ByteArrayValue([]byte(derived.BundledRefundState)), false, nil
	case ColumnDisbursementDate:
		return optionalInt32Value(derived.DisbursementDateDays)
	case ColumnDisbursementAtLocal:
		return optionalInt64Value(derived.DisbursementAtLocalNanos)
	case ColumnCommunicationDate:
		return optionalInt32Value(derived.CommunicationDateDays)
	case ColumnCommunicationAtLocal:
		return optionalInt64Value(derived.CommunicationAtLocalNanos)
	case ColumnPublisherLoadedAtLocal:
		return optionalInt64Value(derived.PublisherLoadedAtNanos)
	case ColumnReportYear:
		return optionalInt64Value(derived.ReportYear)
	case ColumnTwoYearTransactionPeriod:
		return parquet.Int64Value(derived.TwoYearTransactionPeriod), false, nil
	case ColumnMemoedSubtotal:
		return parquet.BooleanValue(derived.MemoedSubtotal), false, nil
	default:
		return parquet.Value{}, false, fmt.Errorf("unsupported Schedule B Parquet column %s", candidate.name)
	}
}

func optionalInt32Value(value *int32) (parquet.Value, bool, error) {
	if value == nil {
		return parquet.Value{}, true, nil
	}
	return parquet.Int32Value(*value), false, nil
}

func optionalInt64Value(value *int64) (parquet.Value, bool, error) {
	if value == nil {
		return parquet.Value{}, true, nil
	}
	return parquet.Int64Value(*value), false, nil
}

// SemanticHasher binds source locators and all 81 decoded values in row order.
// It deliberately excludes typed projections because those are deterministic
// views of the retained source values.
type SemanticHasher struct {
	hash hash.Hash
	rows uint64
}

// NewSemanticHasher creates an empty physical-fact semantic stream.
func NewSemanticHasher() *SemanticHasher {
	hasher := sha256.New()
	writeBytes(hasher, []byte(SemanticDigestVersion))
	return &SemanticHasher{hash: hasher}
}

// AddSource adds one validated source row and exact source locator.
func (hasher *SemanticHasher) AddSource(row *scheduleb.Row, metadata Metadata) error {
	if err := scheduleb.Validate(row, ""); err != nil {
		return err
	}
	if metadata.SourceRowOrdinal != row.Number() {
		return fmt.Errorf("semantic locator does not match source row")
	}
	hasher.addLocator(metadata)
	for index := 0; index < scheduleb.FieldCount; index++ {
		field, ok := row.Field(index)
		if !ok {
			return fmt.Errorf("semantic source field %d is missing", index+1)
		}
		hasher.addValue(field.IsNull(), field.Bytes())
	}
	hasher.rows++
	return nil
}

func (hasher *SemanticHasher) addParquet(row parquet.Row, schema *Schema) error {
	if len(row) != len(schema.bindings) {
		return fmt.Errorf("Parquet row has %d values; want %d", len(row), len(schema.bindings))
	}
	values := make([]parquet.Value, len(row))
	seen := make([]bool, len(row))
	for _, value := range row {
		column := value.Column()
		if column < 0 || column >= len(values) || seen[column] {
			return fmt.Errorf("Parquet row has invalid or repeated column %d", column)
		}
		values[column] = value
		seen[column] = true
	}
	readNonNegative := func(name string) (uint64, error) {
		index := schema.nameToParquet[name]
		value := values[index]
		if value.IsNull() || value.Int64() < 0 {
			return 0, fmt.Errorf("Parquet locator column %s is invalid", name)
		}
		return uint64(value.Int64()), nil
	}
	ordinal, err := readNonNegative(ColumnSourceRowOrdinal)
	if err != nil {
		return err
	}
	offset, err := readNonNegative(ColumnSourceRawByteOffset)
	if err != nil {
		return err
	}
	length, err := readNonNegative(ColumnSourceRawByteLength)
	if err != nil {
		return err
	}
	hasher.addLocator(Metadata{SourceRowOrdinal: ordinal, SourceRawByteOffset: offset, SourceRawByteLength: length})
	for sourceIndex := range schema.columns {
		value := values[schema.sourceToParquet[sourceIndex]]
		if value.IsNull() {
			hasher.addValue(true, nil)
			continue
		}
		hasher.addValue(false, value.ByteArray())
	}
	hasher.rows++
	return nil
}

func (hasher *SemanticHasher) addLocator(metadata Metadata) {
	writeUint64(hasher.hash, metadata.SourceRowOrdinal)
	writeUint64(hasher.hash, metadata.SourceRawByteOffset)
	writeUint64(hasher.hash, metadata.SourceRawByteLength)
}

func (hasher *SemanticHasher) addValue(null bool, value []byte) {
	if null {
		_, _ = hasher.hash.Write([]byte{0})
		return
	}
	_, _ = hasher.hash.Write([]byte{1})
	writeBytes(hasher.hash, value)
}

// Sum returns the digest of all rows added so far.
func (hasher *SemanticHasher) Sum() string { return hex.EncodeToString(hasher.hash.Sum(nil)) }

// Rows returns the number of rows represented by the digest.
func (hasher *SemanticHasher) Rows() uint64 { return hasher.rows }

// Verification is the independent readback result for one immutable shard.
type Verification struct {
	Rows           uint64
	RowGroups      uint64
	SemanticSHA256 string
}

// VerifyFile opens a completed file through the Parquet reader, requires the
// exact physical schema, and recomputes the source-value semantic digest.
func VerifyFile(path string, expected *Schema) (Verification, error) {
	file, err := os.Open(path)
	if err != nil {
		return Verification{}, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return Verification{}, err
	}
	opened, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return Verification{}, err
	}
	if opened.Schema().String() != expected.parquet.String() {
		return Verification{}, fmt.Errorf("Schedule B Parquet schema does not match %s", PhysicalSchemaVersion)
	}
	reader := parquet.NewReader(file)
	defer func() { _ = reader.Close() }()
	hasher := NewSemanticHasher()
	buffer := make([]parquet.Row, 256)
	for {
		count, readErr := reader.ReadRows(buffer)
		for _, row := range buffer[:count] {
			if err := hasher.addParquet(row, expected); err != nil {
				return Verification{}, err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return Verification{}, readErr
		}
	}
	if err := reader.Close(); err != nil {
		return Verification{}, err
	}
	return Verification{Rows: hasher.Rows(), RowGroups: uint64(len(opened.RowGroups())), SemanticSHA256: hasher.Sum()}, nil
}

func writeBytes(destination hash.Hash, value []byte) {
	writeUint64(destination, uint64(len(value)))
	_, _ = destination.Write(value)
}

func writeUint64(destination hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = destination.Write(encoded[:])
}
