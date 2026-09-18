package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/formatter"
	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/ajitpratap0/GoSQLX/pkg/sql/keywords"
)

const schemaFormatVersion = 2

var (
	// namedNotNullRE captures Oracle's named inline NOT NULL form before it is
	// normalized to syntax understood by GoSQLX.
	namedNotNullRE = regexp.MustCompile(`(?i)\bCONSTRAINT\s+((?:"[^"]+"|` + "`[^`]+`" + `|[A-Za-z_][A-Za-z0-9_$#]*))\s+NOT\s+NULL\b`)
	// createIndexRE and simpleIndexRE delimit the narrow expression-index fallback.
	createIndexRE = regexp.MustCompile(`(?is)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+([^\s]+)\s+ON\s+([^\s(]+)\s*\(`)
	auxiliaryRE   = regexp.MustCompile(`(?is)^\s*(CREATE\s+SEQUENCE|CREATE\s+ROLE|GRANT|DROP\s+DATABASE|CREATE\s+DATABASE|USE)\b(?:\s+([^\s;]+))?`)
	simpleIndexRE = regexp.MustCompile(`(?is)^\s*("[^"]+"|` + "`[^`]+`" + `|[A-Za-z_][A-Za-z0-9_$#]*)(?:\s+(ASC|DESC))?\s*$`)
)

// tableState tracks a table while statements are accumulated before validation.
type tableState struct {
	table    Table
	declared bool
}

// builder collects parsed tables, auxiliary statements, and adapter usage.
type builder struct {
	tables     map[string]*tableState
	auxiliary  []AuxiliaryObject
	adapterSet map[string]bool
}

// newBuilder returns an empty parser accumulator with initialized maps.
func newBuilder() *builder {
	return &builder{tables: map[string]*tableState{}, adapterSet: map[string]bool{}}
}

// table returns the canonical table state, creating it on first reference.
func (b *builder) table(name string) *tableState {
	key := canonicalName(name)
	if found := b.tables[key]; found != nil {
		return found
	}
	state := &tableState{table: Table{Name: cleanName(name)}}
	b.tables[key] = state
	return state
}

// parseSchema converts DDL bytes into a sorted, validated Schema model.
func parseSchema(path string, input []byte, dialectName string) (*Schema, error) {
	dialect, normalized, err := parseDialect(dialectName)
	if err != nil {
		return nil, err
	}
	statements, err := splitStatements(string(input))
	if err != nil {
		return nil, fmt.Errorf("split DDL: %w", err)
	}
	b := newBuilder()
	for i, item := range statements {
		// A single malformed or unsupported statement invalidates the complete
		// model; emitting a partial schema would hide missing relationships.
		if err := b.consumeStatement(item, dialect, normalized); err != nil {
			return nil, fmt.Errorf("statement %d at line %d: %w", i+1, item.Line, err)
		}
	}

	sum := sha256.Sum256(input)
	schema := &Schema{
		FormatVersion: schemaFormatVersion,
		Source:        SourceInfo{File: filepath.Base(path), Dialect: normalized, SHA256: hex.EncodeToString(sum[:])},
		Parser:        ParserInfo{Name: "GoSQLX", Version: "v1.14.0"},
		Auxiliary:     b.auxiliary,
	}
	for adapter := range b.adapterSet {
		schema.Parser.Adapters = append(schema.Parser.Adapters, adapter)
	}
	sort.Strings(schema.Parser.Adapters)
	for _, state := range b.tables {
		schema.Tables = append(schema.Tables, state.table)
	}
	sort.Slice(schema.Tables, func(i, j int) bool {
		return canonicalName(schema.Tables[i].Name) < canonicalName(schema.Tables[j].Name)
	})
	for i := range schema.Tables {
		sort.SliceStable(schema.Tables[i].Indexes, func(a, z int) bool {
			return canonicalName(schema.Tables[i].Indexes[a].Name) < canonicalName(schema.Tables[i].Indexes[z].Name)
		})
	}
	sort.SliceStable(schema.Auxiliary, func(i, j int) bool {
		if schema.Auxiliary[i].Kind != schema.Auxiliary[j].Kind {
			return schema.Auxiliary[i].Kind < schema.Auxiliary[j].Kind
		}
		if schema.Auxiliary[i].Name != schema.Auxiliary[j].Name {
			return schema.Auxiliary[i].Name < schema.Auxiliary[j].Name
		}
		return schema.Auxiliary[i].SourceLine < schema.Auxiliary[j].SourceLine
	})
	if err := validateAndResolve(schema, b.tables); err != nil {
		return nil, err
	}
	return schema, nil
}

// parseDialect maps user-facing dialect names to GoSQLX dialect constants.
func parseDialect(name string) (keywords.SQLDialect, string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "oracle":
		return keywords.DialectOracle, "oracle", nil
	case "mysql", "mariadb":
		return keywords.DialectMySQL, "mysql", nil
	default:
		return "", "", fmt.Errorf("unsupported dialect %q (supported: oracle, mysql)", name)
	}
}

// consumeStatement handles one statement, preserving auxiliary DDL and routing
// relational statements through GoSQLX plus narrowly scoped compatibility fallbacks.
func (b *builder) consumeStatement(item statement, dialect keywords.SQLDialect, dialectName string) error {
	if match := auxiliaryRE.FindStringSubmatch(item.SQL); match != nil {
		kind := strings.ToUpper(strings.Join(strings.Fields(match[1]), " "))
		name := ""
		if len(match) > 2 && kind != "GRANT" {
			name = cleanName(match[2])
		}
		b.auxiliary = append(b.auxiliary, AuxiliaryObject{Kind: kind, Name: name, SQL: compactSQL(item.SQL), SourceLine: item.Line})
		return nil
	}

	sqlText := item.SQL
	namedConstraints := map[string][]string{}
	organization := ""
	if startsWithWords(sqlText, "CREATE TABLE") {
		var adapted bool
		var err error
		sqlText, namedConstraints, organization, adapted, err = adaptCreateTable(sqlText, dialectName)
		if err != nil {
			return err
		}
		if adapted {
			b.adapterSet["oracle_named_not_null"] = true
		}
		if organization != "" {
			b.adapterSet["oracle_organization_index"] = true
		}
	}

	tree, err := gosqlx.ParseWithDialect(sqlText, dialect)
	if err != nil {
		// Only expression indexes use the fallback: accepting arbitrary failed
		// SQL here would make the schema appear complete when it is not.
		if startsWithWords(sqlText, "CREATE INDEX") || startsWithWords(sqlText, "CREATE UNIQUE INDEX") {
			idx, table, fallbackErr := parseExpressionIndex(item.SQL, item.Line)
			if fallbackErr == nil {
				b.table(table).table.Indexes = append(b.table(table).table.Indexes, idx)
				b.adapterSet["expression_index"] = true
				return nil
			}
		}
		return fmt.Errorf("GoSQLX parse failed: %v", err)
	}
	// An empty AST is treated as an error rather than silently accepting input.
	if tree == nil || len(tree.Statements) == 0 {
		return fmt.Errorf("GoSQLX returned no AST")
	}
	for _, stmt := range tree.Statements {
		if err := b.consumeAST(stmt, item.Line, namedConstraints, organization); err != nil {
			return err
		}
	}
	return nil
}

// consumeAST transfers one GoSQLX AST statement into the internal model.
func (b *builder) consumeAST(stmt ast.Statement, line int, named map[string][]string, organization string) error {
	switch node := stmt.(type) {
	case *ast.CreateTableStatement:
		// Table declarations establish the source-of-truth columns before
		// later ALTER statements add relationships or indexes.
		state := b.table(node.Name)
		if state.declared {
			return fmt.Errorf("table %s is declared more than once", node.Name)
		}
		state.declared = true
		state.table.SourceLine = line
		state.table.Organization = organization
		for _, column := range node.Columns {
			converted, constraints := convertColumn(column, line)
			attachNamedNotNull(&converted, named[canonicalName(column.Name)])
			state.table.Columns = append(state.table.Columns, converted)
			state.table.Constraints = append(state.table.Constraints, constraints...)
		}
		for _, constraint := range node.Constraints {
			state.table.Constraints = append(state.table.Constraints, convertConstraint(constraint, line))
		}
		return nil
	case *ast.CreateIndexStatement:
		// Indexes are kept separately from constraints because source indexes
		// are reported alongside each FK in the generated registry.
		idx := Index{Name: cleanName(node.Name), Unique: node.Unique, Method: node.Using, SourceLine: line}
		for _, column := range node.Columns {
			idx.Parts = append(idx.Parts, IndexPart{Column: cleanName(column.Column), Direction: column.Direction, Collation: column.Collate})
		}
		if node.Where != nil {
			idx.Where = formatExpr(node.Where)
		}
		b.table(node.Table).table.Indexes = append(b.table(node.Table).table.Indexes, idx)
		return nil
	case *ast.AlterStatement:
		// The adapter intentionally accepts only ALTER TABLE ADD CONSTRAINT;
		// silently ignoring other ALTER operations would lose schema facts.
		if node.Type != ast.AlterTypeTable {
			return fmt.Errorf("unhandled ALTER object type %v", node.Type)
		}
		state := b.table(node.Name)
		op, ok := node.Operation.(*ast.AlterTableOperation)
		if !ok || op == nil {
			return fmt.Errorf("unhandled ALTER TABLE operation %T", node.Operation)
		}
		switch op.Type {
		case ast.AddConstraint:
			if op.Constraint == nil {
				return fmt.Errorf("ADD CONSTRAINT has no constraint")
			}
			state.table.Constraints = append(state.table.Constraints, convertConstraint(*op.Constraint, line))
			return nil
		default:
			return fmt.Errorf("unhandled ALTER TABLE operation %v", op.Type)
		}
	default:
		return fmt.Errorf("unhandled GoSQLX AST statement %T", stmt)
	}
}

// convertColumn maps a GoSQLX column definition and extracts inline constraints.
func convertColumn(column ast.ColumnDef, line int) (Column, []Constraint) {
	out := Column{Name: cleanName(column.Name), Type: column.Type, Nullable: true}
	var constraints []Constraint
	for _, item := range column.Constraints {
		kind := strings.ToUpper(strings.TrimSpace(item.Type))
		out.Constraints = append(out.Constraints, ColumnConstraint{Type: kind})
		switch kind {
		case "NOT NULL", "PRIMARY KEY":
			out.Nullable = false
		}
		if item.Default != nil {
			out.Default = formatExpr(item.Default)
		}
		switch kind {
		case "PRIMARY KEY", "UNIQUE":
			constraints = append(constraints, Constraint{Type: kind, Columns: []string{out.Name}, SourceLine: line})
		case "REFERENCES", "FOREIGN KEY":
			if item.References != nil {
				constraints = append(constraints, Constraint{Type: "FOREIGN KEY", Columns: []string{out.Name}, ReferencedTable: cleanName(item.References.Table), ReferencedColumns: cleanNames(item.References.Columns), OnDelete: item.References.OnDelete, OnUpdate: item.References.OnUpdate, SourceLine: line})
			}
		case "CHECK":
			if item.Check != nil {
				constraints = append(constraints, Constraint{Type: "CHECK", Columns: []string{out.Name}, Expression: formatExpr(item.Check), SourceLine: line})
			}
		}
	}
	return out, constraints
}

// attachNamedNotNull restores names removed when Oracle inline NOT NULL syntax
// is adapted to the parser's ordinary NOT NULL form.
func attachNamedNotNull(column *Column, names []string) {
	for _, name := range names {
		attached := false
		for i := range column.Constraints {
			if column.Constraints[i].Type == "NOT NULL" && column.Constraints[i].Name == "" {
				column.Constraints[i].Name = name
				attached = true
				break
			}
		}
		if !attached {
			column.Constraints = append(column.Constraints, ColumnConstraint{Name: name, Type: "NOT NULL"})
			column.Nullable = false
		}
	}
}

// convertConstraint maps a table-level GoSQLX constraint into the model.
func convertConstraint(item ast.TableConstraint, line int) Constraint {
	out := Constraint{Name: cleanName(item.Name), Type: strings.ToUpper(strings.TrimSpace(item.Type)), Columns: cleanNames(item.Columns), SourceLine: line}
	if item.References != nil {
		out.ReferencedTable = cleanName(item.References.Table)
		out.ReferencedColumns = cleanNames(item.References.Columns)
		out.OnDelete = item.References.OnDelete
		out.OnUpdate = item.References.OnUpdate
	}
	if item.Check != nil {
		out.Expression = formatExpr(item.Check)
	}
	return out
}

// formatExpr renders an AST expression into stable single-line SQL text.
func formatExpr(expr ast.Expression) string {
	if expr == nil {
		return ""
	}
	return strings.TrimSpace(formatter.FormatExpression(expr, ast.FormatOptions{}))
}

// startsWithWords compares a statement prefix after normalizing whitespace.
func startsWithWords(sqlText, prefix string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.Join(strings.Fields(sqlText), " ")), prefix)
}

// canonicalName returns the case-insensitive lookup form used for SQL names.
func canonicalName(value string) string { return strings.ToUpper(cleanName(value)) }

// cleanName removes surrounding SQL quoting while preserving the identifier text.
func cleanName(value string) string {
	return strings.Trim(strings.TrimSpace(value), "`\"")
}

// cleanNames applies cleanName to every identifier in a list.
func cleanNames(values []string) []string {
	out := make([]string, len(values))
	for i := range values {
		out[i] = cleanName(values[i])
	}
	return out
}

// compactSQL preserves auxiliary SQL while making it safe to place on one line.
func compactSQL(value string) string { return strings.Join(strings.Fields(value), " ") }
