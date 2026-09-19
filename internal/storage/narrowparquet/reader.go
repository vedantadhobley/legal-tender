// Package narrowparquet reads named flat columns without source-width temporary
// rows. Callers must independently verify the full source schema and backing.
package narrowparquet

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
)

type rowGroup struct{ parquet.RowGroup }

func (r rowGroup) Rows() parquet.Rows { return parquet.NewRowGroupRowReader(r.RowGroup) }

// New preserves exact node types, nullability and definition levels. Missing,
// nested and repeated source columns fail rather than silently converting.
func New[T any](p *parquet.File) (*parquet.GenericReader[T], error) {
	if p.NumRows() == 0 || len(p.RowGroups()) == 0 {
		return nil, fmt.Errorf("nonempty narrow source required")
	}
	for _, path := range p.Schema().Columns() {
		column, _ := p.Schema().Lookup(path...)
		if len(path) != 1 || column.MaxRepetitionLevel != 0 {
			return nil, fmt.Errorf("flat non-repeated source required")
		}
	}
	target := parquet.SchemaOf(new(T))
	for _, path := range target.Columns() {
		want, _ := target.Lookup(path...)
		got, ok := p.Schema().Lookup(path...)
		if len(path) != 1 || !ok || !parquet.EqualNodes(got.Node, want.Node) || got.MaxDefinitionLevel != want.MaxDefinitionLevel {
			return nil, fmt.Errorf("narrow column %v missing or incompatible", path)
		}
	}
	conversion, err := parquet.Convert(target, p.Schema())
	if err != nil {
		return nil, err
	}
	group := parquet.ConvertRowGroup(parquet.MultiRowGroup(p.RowGroups()...), conversion)
	return parquet.NewGenericRowGroupReader[T](rowGroup{group}), nil
}
