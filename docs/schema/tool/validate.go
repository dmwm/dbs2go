package main

import (
	"fmt"
	"sort"
	"strings"
)

// validateAndResolve checks internal references and expands validated table
// constraints into the schema-wide ForeignKeys registry.
func validateAndResolve(schema *Schema, states map[string]*tableState) error {
	// First pass builds the table/column index and checks local references.
	byName := make(map[string]*Table, len(schema.Tables))
	for i := range schema.Tables {
		table := &schema.Tables[i]
		state := states[canonicalName(table.Name)]
		if state == nil || !state.declared {
			return fmt.Errorf("table %s was referenced but never declared", table.Name)
		}
		if len(table.Columns) == 0 {
			return fmt.Errorf("table %s has no parsed columns", table.Name)
		}
		if _, duplicate := byName[canonicalName(table.Name)]; duplicate {
			return fmt.Errorf("duplicate table %s", table.Name)
		}
		byName[canonicalName(table.Name)] = table
		columns := map[string]bool{}
		for _, column := range table.Columns {
			key := canonicalName(column.Name)
			if key == "" || columns[key] {
				return fmt.Errorf("table %s has an empty or duplicate column %q", table.Name, column.Name)
			}
			columns[key] = true
		}
		for _, constraint := range table.Constraints {
			for _, column := range constraint.Columns {
				if !columns[canonicalName(column)] {
					return fmt.Errorf("constraint %s on %s references missing source column %s", constraint.Name, table.Name, column)
				}
			}
		}
		for _, index := range table.Indexes {
			if len(index.Parts) == 0 {
				return fmt.Errorf("index %s on %s has no parts", index.Name, table.Name)
			}
			for _, part := range index.Parts {
				if part.Column != "" && !columns[canonicalName(part.Column)] {
					return fmt.Errorf("index %s on %s references missing column %s", index.Name, table.Name, part.Column)
				}
				if (part.Column == "") == (part.Expression == "") {
					return fmt.Errorf("index %s on %s has an invalid part", index.Name, table.Name)
				}
			}
		}
	}

	// Resolve foreign keys only after every table and column has been indexed;
	// this prevents forward references from being mistaken for missing objects.
	for ti := range schema.Tables {
		table := &schema.Tables[ti]
		for ci := range table.Constraints {
			constraint := &table.Constraints[ci]
			if constraint.Type != "FOREIGN KEY" {
				continue
			}
			if len(constraint.Columns) == 0 || len(constraint.Columns) != len(constraint.ReferencedColumns) {
				return fmt.Errorf("foreign key %s on %s has mismatched source and target columns", constraint.Name, table.Name)
			}
			target := byName[canonicalName(constraint.ReferencedTable)]
			if target == nil {
				return fmt.Errorf("foreign key %s on %s references missing table %s", constraint.Name, table.Name, constraint.ReferencedTable)
			}
			for _, name := range constraint.ReferencedColumns {
				if !hasColumn(*target, name) {
					return fmt.Errorf("foreign key %s on %s references missing column %s.%s", constraint.Name, table.Name, target.Name, name)
				}
			}
			constraint.ReferencedConstraint = referencedConstraint(*target, constraint.ReferencedColumns)
			if constraint.ReferencedConstraint == "" {
				return fmt.Errorf("foreign key %s on %s does not reference an exact primary/unique key on %s", constraint.Name, table.Name, target.Name)
			}
			schema.ForeignKeys = append(schema.ForeignKeys, ForeignKey{
				Constraint: constraint.Name, SourceTable: table.Name, SourceColumns: append([]string(nil), constraint.Columns...),
				TargetTable: target.Name, TargetColumns: append([]string(nil), constraint.ReferencedColumns...),
				ReferencedConstraint: constraint.ReferencedConstraint, OnDelete: constraint.OnDelete, OnUpdate: constraint.OnUpdate, SourceLine: constraint.SourceLine,
			})
		}
	}
	sort.Slice(schema.ForeignKeys, func(i, j int) bool {
		left, right := schema.ForeignKeys[i], schema.ForeignKeys[j]
		if left.SourceTable != right.SourceTable {
			return left.SourceTable < right.SourceTable
		}
		if left.Constraint != right.Constraint {
			return left.Constraint < right.Constraint
		}
		return strings.Join(left.SourceColumns, "\x00") < strings.Join(right.SourceColumns, "\x00")
	})
	return nil
}

// hasColumn reports whether a table contains an identifier, case-insensitively.
func hasColumn(table Table, name string) bool {
	for _, column := range table.Columns {
		if canonicalName(column.Name) == canonicalName(name) {
			return true
		}
	}
	return false
}

// referencedConstraint finds the exact PK/UQ constraint targeted by an FK.
func referencedConstraint(table Table, columns []string) string {
	wanted := canonicalColumns(columns)
	for _, constraint := range table.Constraints {
		if (constraint.Type == "PRIMARY KEY" || constraint.Type == "UNIQUE") && canonicalColumns(constraint.Columns) == wanted {
			if constraint.Name != "" {
				return constraint.Name
			}
			return "<unnamed " + strings.ToLower(constraint.Type) + ">"
		}
	}
	return ""
}

// canonicalColumns creates a stable comparison key for ordered column lists.
func canonicalColumns(columns []string) string {
	out := make([]string, len(columns))
	for i, column := range columns {
		out[i] = canonicalName(column)
	}
	return strings.Join(out, "\x00")
}
