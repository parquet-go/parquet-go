package parquet

import "math"

// LeafColumn is a struct type representing leaf columns of a parquet schema.
type LeafColumn struct {
	Node               Node
	Path               []string
	ColumnIndex        int
	MaxRepetitionLevel int
	MaxDefinitionLevel int
}

func columnMappingOf(schema Node) (mapping columnMappingGroup, columns [][]string) {
	mapping = make(columnMappingGroup)
	columns = make([][]string, 0, 16)

	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		path := make(columnPath, len(leaf.path))
		copy(path, leaf.path)
		columns = append(columns, path)

		group := mapping
		for len(path) > 1 {
			columnName := path[0]
			g, ok := group[columnName].(columnMappingGroup)
			if !ok {
				g = make(columnMappingGroup)
				group[columnName] = g
			}
			group, path = g, path[1:]
		}

		leaf.path = path // use the copy
		group[path[0]] = &columnMappingLeaf{column: leaf}
	})

	return mapping, columns
}

type columnMapping interface {
	lookup(path columnPath) leafColumn
}

type columnMappingGroup map[string]columnMapping

func (group columnMappingGroup) lookup(path columnPath) leafColumn {
	if len(path) > 0 {
		c, ok := group[path[0]]
		if ok {
			return c.lookup(path[1:])
		}
	}
	return leafColumn{columnIndex: math.MaxUint16}
}

// firstLeaf returns the leaf under group with the lowest column index.
func (group columnMappingGroup) firstLeaf() leafColumn {
	first := leafColumn{columnIndex: math.MaxUint16}
	for _, child := range group {
		leaf := leafColumn{columnIndex: math.MaxUint16}
		switch c := child.(type) {
		case *columnMappingLeaf:
			leaf = c.column
		case columnMappingGroup:
			leaf = c.firstLeaf()
		}
		if leaf.columnIndex < first.columnIndex {
			first = leaf
		}
	}
	return first
}

type columnMappingLeaf struct {
	column leafColumn
}

func (leaf *columnMappingLeaf) lookup(path columnPath) leafColumn {
	if len(path) == 0 {
		return leaf.column
	}
	return leafColumn{columnIndex: math.MaxUint16}
}
