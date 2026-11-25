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

// descend returns the group at path, or nil if a component is missing or is a leaf.
func (group columnMappingGroup) descend(path columnPath) columnMappingGroup {
	for _, name := range path {
		next, ok := group[name].(columnMappingGroup)
		if !ok {
			return nil
		}
		group = next
	}
	return group
}

// hasDirectLeaf reports whether this group has a leaf child.
func (group columnMappingGroup) hasDirectLeaf() bool {
	for _, child := range group {
		if _, ok := child.(*columnMappingLeaf); ok {
			return true
		}
	}
	return false
}

// lookupAnyLeaf returns the lowest-index leaf in this group, including leaves
// nested under child groups. A leaf with a nil node is returned when the group
// has no leaves.
func (group columnMappingGroup) lookupAnyLeaf() leafColumn {
	best := leafColumn{columnIndex: math.MaxUint16}
	var walk func(columnMapping)
	walk = func(node columnMapping) {
		switch node := node.(type) {
		case *columnMappingLeaf:
			if node.column.columnIndex < best.columnIndex {
				best = node.column
			}
		case columnMappingGroup:
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(group)
	return best
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
