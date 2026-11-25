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

// columnTemplate is the leaf used to count instances of one source group.
// direct is set when that leaf is a child of the group. Otherwise leaf is
// some leaf under the group, which still carries the group's repetition.
type columnTemplate struct {
	leaf   leafColumn
	direct bool
	has    bool
	groups map[string]*columnTemplate
}

// templates indexes every source group by the leaf Convert should read when a
// target column added under that group is missing from the source.
func (group columnMappingGroup) templates() *columnTemplate {
	t := &columnTemplate{}
	var directName string
	var nested leafColumn
	hasNested := false
	for name, child := range group {
		switch c := child.(type) {
		case *columnMappingLeaf:
			if !t.direct || name < directName {
				t.leaf = c.column
				t.direct = true
				t.has = true
				directName = name
			}
		case columnMappingGroup:
			childT := c.templates()
			if t.groups == nil {
				t.groups = make(map[string]*columnTemplate)
			}
			t.groups[name] = childT
			if childT.has && (!hasNested || childT.leaf.columnIndex < nested.columnIndex) {
				nested = childT.leaf
				hasNested = true
			}
		}
	}
	if !t.direct && hasNested {
		t.leaf = nested
		t.has = true
	}
	return t
}

func (t *columnTemplate) at(path columnPath) *columnTemplate {
	cur := t
	for _, name := range path {
		if cur == nil || cur.groups == nil {
			return nil
		}
		cur = cur.groups[name]
	}
	return cur
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
