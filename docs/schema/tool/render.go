package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// schemaGroup defines a functional area and its visual identity.
type schemaGroup struct {
	Key, Title, Description, Fill, Stroke string
}

// schemaGroups is the canonical ordering shared by every rendered atlas.
var schemaGroups = []schemaGroup{
	{"core", "Core data and containment", "Datasets, blocks, files, runs, and luminosity sections.", "#E6F5ED", "#17734D"},
	{"class", "Classification and lookup", "Dataset identity, eras, tiers, types, physics groups, and branch hashes.", "#E9F2FC", "#245FA9"},
	{"parent", "Parentage and associations", "Dataset, block, and file provenance links.", "#F3EAFF", "#7047AA"},
	{"config", "Processing configuration", "Applications, releases, parameter sets, and output-module associations.", "#FFF0DF", "#AC5516"},
	{"ops", "Migration and instance metadata", "Migration requests and DBS schema/version metadata.", "#E8F4F5", "#147887"},
	{"other", "Other operational tables", "Dialect-specific operational tables outside the common Oracle model.", "#F1F3F5", "#596675"},
}

// RenderOptions selects the visualization mode, layout density, and SVG links.
type RenderOptions struct {
	Visualizations map[string]bool
	Layout         string
	SVGReferences  map[string]string
}

// renderJSON serializes the validated model as deterministic indented JSON.
func renderJSON(schema *Schema) ([]byte, error) {
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// renderMarkdown produces the default compact domain atlas.
func renderMarkdown(schema *Schema) string {
	return renderMarkdownWithOptions(schema, RenderOptions{Visualizations: map[string]bool{"domains": true}, Layout: "compact"})
}

// renderMarkdownWithOptions renders the shared atlas while varying only its
// selected relational-map subsection content.
func renderMarkdownWithOptions(schema *Schema, options RenderOptions) string {
	if options.Layout == "" {
		options.Layout = "compact"
	}
	if len(options.Visualizations) == 0 {
		options.Visualizations = map[string]bool{"domains": true}
	}
	var b strings.Builder
	stats := schemaStats(schema)
	b.WriteString("# DBS relational data atlas\n\n")
	fmt.Fprintf(&b, "> Generated from [`%s`](../../static/schema/DDL/%s) (%s, SHA-256 `%s`) with %s %s. This is the persisted database structure defined by the DDL, not a live database inventory.\n\n", schema.Source.File, schema.Source.File, schema.Source.Dialect, schema.Source.SHA256, schema.Parser.Name, schema.Parser.Version)
	fmt.Fprintf(&b, "**%d tables · %d columns · %d primary keys · %d unique constraints · %d checks · %d foreign keys · %d explicit indexes**\n\n", len(schema.Tables), columnCount(schema), stats.PrimaryKeys, stats.Unique, stats.Checks, len(schema.ForeignKeys), stats.Indexes)
	b.WriteString("[Legend](#how-to-read-this-atlas) · [Architecture](#architecture) · [Relational maps](#relational-maps) · [Foreign keys](#foreign-key-registry) · [Table dictionary](#table-dictionary) · [Other database objects](#other-database-objects)\n\n")
	b.WriteString(renderLegend(options.Layout))

	b.WriteString("## Architecture\n\n")
	b.WriteString("The schema has five functional areas. The central structural path is dataset → block → file → luminosity section; lookup tables classify those records, parentage tables link provenance, configuration tables describe producing software, and migration tables record transfers.\n\n")
	b.WriteString(renderArchitecture(options.Layout))
	b.WriteString("\n")

	b.WriteString("## Whole-schema relations\n\n")
	b.WriteString("This compact overview shows every table and relationship at table level. Exact column endpoints, constraint names, delete actions, and keys are preserved in the selected maps, registry, and dictionary below.\n\n")
	b.WriteString(renderOverview(schema, options.Layout))
	b.WriteString("\n")

	b.WriteString("## Relational maps\n\n")
	b.WriteString("The whole-schema graph and five functional areas below use the selected visualization mode. All surrounding documentation is mode-independent.\n\n")
	fmt.Fprintf(&b, "<details>\n<summary><strong>Whole-schema relations</strong></summary>\n\nThe whole-schema relationship graph rendered with the selected visualization mode.\n\n%s\n</details>\n\n", renderVisualizationWholeSchema(schema, options))
	for _, group := range schemaGroups {
		if group.Key == "other" {
			continue
		}
		fmt.Fprintf(&b, "<details>\n<summary><strong>%s</strong></summary>\n\n", group.Title)
		fmt.Fprintf(&b, "%s\n\n", group.Description)
		b.WriteString(renderVisualizationGroup(schema, group, options))
		b.WriteString("\n</details>\n\n")
	}

	b.WriteString("## Foreign-key registry\n\n")
	b.WriteString("The compact registry preserves the constraint, exact endpoints, delete behavior, referenced key, and source-side index.\n\n")
	for _, group := range schemaGroups {
		fks := foreignKeysForGroup(schema.ForeignKeys, group.Key)
		if len(fks) == 0 {
			continue
		}
		fmt.Fprintf(&b, "<details>\n<summary><strong>%s</strong> — %s</summary>\n\n", group.Title, countNoun(len(fks), "foreign key"))
		b.WriteString("| FK | Source → referenced endpoint | Delete | Referenced key / source index |\n|---|---|---|---|\n")
		for _, fk := range fks {
			source := fmt.Sprintf("`%s.%s`", fk.SourceTable, strings.Join(fk.SourceColumns, ", "))
			target := fmt.Sprintf("`%s.%s`", fk.TargetTable, strings.Join(fk.TargetColumns, ", "))
			fmt.Fprintf(&b, "| `%s` | %s →<br>%s | %s | `%s`<br>%s |\n", md(fk.Constraint), source, target, md(deleteRule(fk.OnDelete)), md(fk.ReferencedConstraint), md(sourceIndex(schema, fk)))
		}
		b.WriteString("\n</details>\n\n")
	}

	b.WriteString("## Table dictionary\n\n")
	b.WriteString("Every native column, constraint, index, and incoming/outgoing relationship is present. Tables are collapsed by default so the page remains usable.\n\n")
	for _, group := range schemaGroups {
		members := tablesInGroup(schema, group.Key)
		if len(members) == 0 {
			continue
		}
		fmt.Fprintf(&b, "### %s\n\n", group.Title)
		for _, table := range members {
			renderTableDetails(&b, schema, table, group)
		}
	}

	b.WriteString("## Other database objects\n\n")
	b.WriteString("These statements are outside the relational table graph but remain part of the source DDL. They are preserved instead of being reported as parser failures.\n\n")
	renderAuxiliary(&b, schema.Auxiliary)

	b.WriteString("## Generation and scope\n\n")
	b.WriteString("The source DDL is parsed into a validated model before any output is written. Missing tables or columns, unresolved foreign keys, non-key FK targets, duplicate declarations, and unknown relational statements are fatal. Oracle named `NOT NULL` constraints, index-organized tables, and function indexes are retained through explicit compatibility adapters around GoSQLX.\n")
	return b.String()
}

// renderVisualizationWholeSchema renders exactly one whole-schema view for a mode.
func renderVisualizationWholeSchema(schema *Schema, options RenderOptions) string {
	switch {
	case options.Visualizations["svg"]:
		ref := options.SVGReferences["whole"]
		if ref == "" {
			return "SVG asset unavailable."
		}
		return fmt.Sprintf("[Open the whole-schema SVG at full size](%s)\n\n![Whole-schema relational SVG](%s)", ref, ref)
	case options.Visualizations["er"]:
		return renderERWholeDiagram(schema, options.Layout)
	default:
		return renderOverview(schema, options.Layout)
	}
}

// renderVisualizationGroup renders one of the five functional-area views.
func renderVisualizationGroup(schema *Schema, group schemaGroup, options RenderOptions) string {
	members := tablesInGroup(schema, group.Key)
	if len(members) == 0 {
		return "No tables in this functional area."
	}
	fks := foreignKeysForGroup(schema.ForeignKeys, group.Key)
	switch {
	case options.Visualizations["svg"]:
		ref := options.SVGReferences[group.Key]
		if ref == "" {
			return "SVG asset unavailable."
		}
		return fmt.Sprintf("[Open the SVG at full size](%s)\n\n![%s relational SVG](%s)", ref, group.Title, ref)
	case options.Visualizations["er"]:
		return renderERDiagram(schema, group, options.Layout)
	default:
		return renderDomainMap(schema, group, members, fks, options.Layout, false)
	}
}

// stats contains the constraint and index counts shown in the atlas header.
type stats struct{ PrimaryKeys, Unique, Checks, Indexes int }

// schemaStats counts table-level objects used by the Markdown summary.
func schemaStats(schema *Schema) stats {
	var out stats
	for _, table := range schema.Tables {
		out.Indexes += len(table.Indexes)
		for _, constraint := range table.Constraints {
			switch constraint.Type {
			case "PRIMARY KEY":
				out.PrimaryKeys++
			case "UNIQUE":
				out.Unique++
			case "CHECK":
				out.Checks++
			}
		}
	}
	return out
}

// renderArchitecture emits the stable high-level Mermaid architecture sketch.
func renderArchitecture(layout string) string {
	return "```mermaid\n" + mermaidDirective(layout) + "\n" +
		"flowchart LR\n" +
		"  DS[\"DATASETS<br/>published dataset identity\"] -->|DS_BK| BK[\"BLOCKS<br/>transfer and storage unit\"]\n" +
		"  DS -->|DS_FL| FL[\"FILES<br/>physical data files\"]\n" +
		"  BK -->|BK_FL| FL\n" +
		"  FL -->|FL_FLM| LM[\"FILE_LUMIS<br/>run / lumi content\"]\n" +
		"  DS -->|DS_DR| DR[\"DATASET_RUNS<br/>run membership\"]\n" +
		"  classDef core fill:#E6F5ED,stroke:#17734D,color:#1f2937;\n" +
		"  class DS,BK,FL,LM,DR core;\n" +
		"```\n"
}

// renderOverview emits the table-level whole-schema Mermaid relationship graph.
func renderOverview(schema *Schema, layout string) string {
	var b strings.Builder
	b.WriteString("```mermaid\n" + mermaidDirective(layout) + "\nflowchart TB\n")
	for _, group := range schemaGroups {
		members := tablesInGroup(schema, group.Key)
		if len(members) == 0 {
			continue
		}
		fmt.Fprintf(&b, "  subgraph %s[\"%s\"]\n", mermaidID("group_"+group.Key), mermaidText(group.Title))
		for _, table := range members {
			pk := primaryKey(table)
			fmt.Fprintf(&b, "    %s[\"%s<br/>%d columns · PK %s\"]\n", mermaidID("table_"+table.Name), mermaidText(table.Name), len(table.Columns), mermaidText(pk.Name))
		}
		b.WriteString("  end\n")
	}
	var orderedTables []string
	for _, group := range schemaGroups {
		for _, table := range tablesInGroup(schema, group.Key) {
			orderedTables = append(orderedTables, mermaidID("table_"+table.Name))
		}
	}
	// Invisible links impose a deterministic vertical sequence without adding
	// visible relationships to the architecture graph.
	writeMermaidVerticalOrder(&b, orderedTables)
	for _, edge := range aggregateOverviewEdges(schema.ForeignKeys) {
		b.WriteString(edge + "\n")
	}
	for _, group := range schemaGroups {
		for _, table := range tablesInGroup(schema, group.Key) {
			fmt.Fprintf(&b, "  style %s fill:%s,stroke:%s,color:#1f2937\n", mermaidID("table_"+table.Name), group.Fill, group.Stroke)
		}
	}
	b.WriteString("```")
	return b.String()
}

// renderDomainMap emits a functional-area Mermaid graph with exact column endpoints.
func renderDomainMap(schema *Schema, group schemaGroup, members []Table, fks []ForeignKey, layout string, allColumns bool) string {
	tableSet := map[string]bool{}
	for _, table := range members {
		tableSet[canonicalName(table.Name)] = true
	}
	for _, fk := range fks {
		tableSet[canonicalName(fk.SourceTable)] = true
		tableSet[canonicalName(fk.TargetTable)] = true
	}
	var included []Table
	for _, table := range schema.Tables {
		if tableSet[canonicalName(table.Name)] {
			included = append(included, table)
		}
	}

	var b strings.Builder
	b.WriteString("```mermaid\n" + mermaidDirective(layout) + "\nflowchart " + domainDirection(group.Key) + "\n")
	var orderedSubgraphs []string
	for _, table := range included {
		columns := mapColumnsFor(table, fks, allColumns)
		header := mermaidTableHeader(table, columns)
		fmt.Fprintf(&b, "  subgraph %s[\"%s\"]\n    direction TB\n", mermaidID("sg_"+table.Name), mermaidText(header))
		orderedSubgraphs = append(orderedSubgraphs, mermaidID("sg_"+table.Name))
		for _, column := range columns {
			fmt.Fprintf(&b, "    %s[\"%s\"]\n", mermaidID(table.Name+"__"+column.Name), mermaidText(columnLabel(table, column, fks)))
		}
		b.WriteString("  end\n")
		style := groupForTable(table.Name)
		fmt.Fprintf(&b, "  style %s fill:%s,stroke:%s,color:#1f2937\n", mermaidID("sg_"+table.Name), style.Fill, style.Stroke)
	}
	writeMermaidVerticalOrder(&b, orderedSubgraphs)
	for _, fk := range fks {
		for i, source := range fk.SourceColumns {
			fmt.Fprintf(&b, "  %s -->|\"%s · %s\"| %s\n", mermaidID(fk.SourceTable+"__"+source), mermaidText(fk.Constraint), mermaidText(deleteRule(fk.OnDelete)), mermaidID(fk.TargetTable+"__"+fk.TargetColumns[i]))
		}
	}
	b.WriteString("```")
	return b.String()
}

// mapColumnsFor selects key and relationship-bearing columns unless all is true.
func mapColumnsFor(table Table, fks []ForeignKey, all bool) []Column {
	if all {
		return table.Columns
	}
	wanted := map[string]bool{}
	for _, column := range primaryKey(table).Columns {
		wanted[canonicalName(column)] = true
	}
	for _, constraint := range table.Constraints {
		if constraint.Type == "UNIQUE" {
			for _, column := range constraint.Columns {
				wanted[canonicalName(column)] = true
			}
		}
	}
	for _, fk := range fks {
		if canonicalName(fk.SourceTable) == canonicalName(table.Name) {
			for _, column := range fk.SourceColumns {
				wanted[canonicalName(column)] = true
			}
		}
		if canonicalName(fk.TargetTable) == canonicalName(table.Name) {
			for _, column := range fk.TargetColumns {
				wanted[canonicalName(column)] = true
			}
		}
	}
	var out []Column
	for _, column := range table.Columns {
		if wanted[canonicalName(column.Name)] {
			out = append(out, column)
		}
	}
	return out
}

// columnLabel formats a Mermaid column node with its semantic badges.
func columnLabel(table Table, column Column, fks []ForeignKey) string {
	var badges []string
	if containsName(primaryKey(table).Columns, column.Name) {
		badges = append(badges, "PK")
	}
	for _, fk := range fks {
		if canonicalName(fk.SourceTable) == canonicalName(table.Name) && containsName(fk.SourceColumns, column.Name) {
			badges = appendOnce(badges, "FK")
		}
	}
	for _, constraint := range table.Constraints {
		if constraint.Type == "UNIQUE" && containsName(constraint.Columns, column.Name) {
			badges = appendOnce(badges, "UK")
		}
	}
	if !column.Nullable {
		badges = appendOnce(badges, "NN")
	}
	label := column.Name + "<br/>" + column.Type
	if len(badges) != 0 {
		label += " · " + strings.Join(badges, " · ")
	}
	return label
}

// renderTableDetails emits the complete collapsed dictionary entry for one table.
func renderTableDetails(b *strings.Builder, schema *Schema, table Table, group schemaGroup) {
	pk := primaryKey(table)
	incoming, outgoing := relationshipDirections(schema.ForeignKeys, table.Name)
	fmt.Fprintf(b, "<details>\n<summary><strong><code>%s</code></strong> — %d columns · PK %s · %d outbound / %d inbound FKs</summary>\n\n", table.Name, len(table.Columns), pk.Name, len(outgoing), len(incoming))
	fmt.Fprintf(b, "%s\n\n", group.Description)
	b.WriteString("| Column | Type | Properties |\n|---|---|---|\n")
	for _, column := range table.Columns {
		var properties []string
		if containsName(pk.Columns, column.Name) {
			properties = append(properties, "`PK`")
		}
		if isForeignKeyColumn(outgoing, column.Name) {
			properties = append(properties, "`FK`")
		}
		if uniqueColumn(table, column.Name) {
			properties = append(properties, "`UQ`")
		}
		if !column.Nullable {
			properties = append(properties, "`NN`")
		}
		if column.Default != "" {
			properties = append(properties, "default `"+column.Default+"`")
		}
		if len(properties) == 0 {
			properties = append(properties, "—")
		}
		fmt.Fprintf(b, "| `%s` | `%s` | %s |\n", md(column.Name), md(column.Type), strings.Join(properties, " "))
	}

	b.WriteString("\n**Keys and constraints**\n\n")
	if len(table.Constraints) == 0 {
		b.WriteString("- None.\n")
	}
	for _, constraint := range table.Constraints {
		detail := strings.Join(constraint.Columns, ", ")
		if constraint.Type == "CHECK" {
			detail = constraint.Expression
		}
		if constraint.Type == "FOREIGN KEY" {
			detail += " → " + constraint.ReferencedTable + "(" + strings.Join(constraint.ReferencedColumns, ", ") + ") · " + deleteRule(constraint.OnDelete)
		}
		fmt.Fprintf(b, "- `%s` **%s** — `%s`\n", md(constraint.Name), constraint.Type, md(detail))
	}
	if len(table.Indexes) != 0 {
		b.WriteString("\n**Explicit indexes**\n\n")
		for _, index := range table.Indexes {
			fmt.Fprintf(b, "- `%s`%s — `%s`\n", md(index.Name), ternary(index.Unique, " **UNIQUE**", ""), md(indexParts(index)))
		}
	}
	if len(outgoing)+len(incoming) != 0 {
		b.WriteString("\n**Relationships**\n\n")
		for _, fk := range outgoing {
			fmt.Fprintf(b, "- Outbound `%s`: `%s` → `%s.%s` (%s)\n", md(fk.Constraint), md(strings.Join(fk.SourceColumns, ", ")), md(fk.TargetTable), md(strings.Join(fk.TargetColumns, ", ")), deleteRule(fk.OnDelete))
		}
		for _, fk := range incoming {
			fmt.Fprintf(b, "- Inbound `%s`: `%s.%s` → `%s`\n", md(fk.Constraint), md(fk.SourceTable), md(strings.Join(fk.SourceColumns, ", ")), md(strings.Join(fk.TargetColumns, ", ")))
		}
	}
	if table.Organization != "" {
		fmt.Fprintf(b, "\nStorage organization: `%s`.\n", table.Organization)
	}
	b.WriteString("\n</details>\n\n")
}

// renderAuxiliary preserves non-table DDL in grouped collapsed sections.
func renderAuxiliary(b *strings.Builder, objects []AuxiliaryObject) {
	byKind := map[string][]AuxiliaryObject{}
	for _, object := range objects {
		byKind[object.Kind] = append(byKind[object.Kind], object)
	}
	for _, kind := range []string{"CREATE SEQUENCE", "CREATE ROLE", "GRANT", "DROP DATABASE", "CREATE DATABASE", "USE"} {
		items := byKind[kind]
		if len(items) == 0 {
			continue
		}
		fmt.Fprintf(b, "<details>\n<summary><strong>%s</strong> — %d statements</summary>\n\n", titleCase(kind), len(items))
		for _, item := range items {
			fmt.Fprintf(b, "- Line %d: `%s`\n", item.SourceLine, strings.ReplaceAll(item.SQL, "`", "\\`"))
		}
		b.WriteString("\n</details>\n\n")
	}
}

// primaryKey returns the table primary key or a placeholder when absent.
func primaryKey(table Table) Constraint {
	for _, constraint := range table.Constraints {
		if constraint.Type == "PRIMARY KEY" {
			return constraint
		}
	}
	return Constraint{Name: "—"}
}

// uniqueColumn reports whether a column participates in a unique constraint.
func uniqueColumn(table Table, column string) bool {
	for _, constraint := range table.Constraints {
		if constraint.Type == "UNIQUE" && containsName(constraint.Columns, column) {
			return true
		}
	}
	return false
}

// isForeignKeyColumn reports whether a column is an FK source column.
func isForeignKeyColumn(fks []ForeignKey, column string) bool {
	for _, fk := range fks {
		if containsName(fk.SourceColumns, column) {
			return true
		}
	}
	return false
}

// containsName compares SQL identifiers case-insensitively.
func containsName(values []string, wanted string) bool {
	for _, value := range values {
		if canonicalName(value) == canonicalName(wanted) {
			return true
		}
	}
	return false
}

// appendOnce appends a marker only when it is not already present.
func appendOnce(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// sourceIndex identifies explicit or primary-key indexes covering an FK prefix.
func sourceIndex(schema *Schema, fk ForeignKey) string {
	for _, table := range schema.Tables {
		if canonicalName(table.Name) != canonicalName(fk.SourceTable) {
			continue
		}
		var names []string
		for _, index := range table.Indexes {
			var columns []string
			for _, part := range index.Parts {
				if part.Column == "" {
					columns = nil
					break
				}
				columns = append(columns, part.Column)
			}
			if hasPrefixColumns(columns, fk.SourceColumns) {
				names = append(names, "index "+index.Name)
			}
		}
		pk := primaryKey(table)
		if hasPrefixColumns(pk.Columns, fk.SourceColumns) {
			names = append(names, "leading columns of "+pk.Name)
		}
		if len(names) != 0 {
			return strings.Join(names, ", ")
		}
	}
	return "no matching explicit source index"
}

// hasPrefixColumns reports whether prefix matches the leading columns in have.
func hasPrefixColumns(have, prefix []string) bool {
	if len(prefix) == 0 || len(have) < len(prefix) {
		return false
	}
	for i := range prefix {
		if canonicalName(have[i]) != canonicalName(prefix[i]) {
			return false
		}
	}
	return true
}

// indexParts formats column and expression parts for dictionary output.
func indexParts(index Index) string {
	var out []string
	for _, part := range index.Parts {
		value := part.Column
		if part.Expression != "" {
			value = part.Expression
		}
		if part.Direction != "" {
			value += " " + part.Direction
		}
		out = append(out, value)
	}
	return strings.Join(out, ", ")
}

// tablesInGroup returns tables assigned to a functional area in schema order.
func tablesInGroup(schema *Schema, key string) []Table {
	var out []Table
	for _, table := range schema.Tables {
		if tableGroupKey(table.Name) == key {
			out = append(out, table)
		}
	}
	return out
}

// foreignKeysForGroup returns relationships assigned to a functional area.
func foreignKeysForGroup(all []ForeignKey, group string) []ForeignKey {
	var out []ForeignKey
	for _, fk := range all {
		if foreignKeyGroup(fk) == group {
			out = append(out, fk)
		}
	}
	return out
}

// foreignKeyGroup assigns an FK to the target classification or source domain.
func foreignKeyGroup(fk ForeignKey) string {
	if tableGroupKey(fk.TargetTable) == "class" {
		return "class"
	}
	switch tableGroupKey(fk.SourceTable) {
	case "parent":
		return "parent"
	case "config":
		return "config"
	case "ops":
		return "ops"
	case "other":
		return "other"
	default:
		return "core"
	}
}

// relationshipDirections partitions a table's relationships by arrow direction.
func relationshipDirections(all []ForeignKey, table string) (incoming, outgoing []ForeignKey) {
	for _, fk := range all {
		if canonicalName(fk.SourceTable) == canonicalName(table) {
			outgoing = append(outgoing, fk)
		}
		if canonicalName(fk.TargetTable) == canonicalName(table) {
			incoming = append(incoming, fk)
		}
	}
	return incoming, outgoing
}

// tableGroupKey maps a table name to one of the atlas functional areas.
func tableGroupKey(name string) string {
	groups := map[string]string{
		"DATASETS": "core", "BLOCKS": "core", "FILES": "core", "FILE_LUMIS": "core", "DATASET_RUNS": "core",
		"PRIMARY_DS_TYPES": "class", "PRIMARY_DATASETS": "class", "PROCESSED_DATASETS": "class", "DATA_TIERS": "class", "DATASET_ACCESS_TYPES": "class", "ACQUISITION_ERAS": "class", "PROCESSING_ERAS": "class", "PHYSICS_GROUPS": "class", "FILE_DATA_TYPES": "class", "BRANCH_HASHES": "class",
		"DATASET_PARENTS": "parent", "BLOCK_PARENTS": "parent", "FILE_PARENTS": "parent", "ASSOCIATED_FILES": "parent",
		"APPLICATION_EXECUTABLES": "config", "RELEASE_VERSIONS": "config", "PARAMETER_SET_HASHES": "config", "OUTPUT_MODULE_CONFIGS": "config", "DATASET_OUTPUT_MOD_CONFIGS": "config", "FILE_OUTPUT_MOD_CONFIGS": "config",
		"MIGRATION_REQUESTS": "ops", "MIGRATION_BLOCKS": "ops", "DBS_VERSIONS": "ops",
	}
	if key := groups[canonicalName(name)]; key != "" {
		return key
	}
	return "other"
}

// groupForTable returns the complete visual metadata for a table's area.
func groupForTable(name string) schemaGroup {
	key := tableGroupKey(name)
	for _, group := range schemaGroups {
		if group.Key == key {
			return group
		}
	}
	return schemaGroups[len(schemaGroups)-1]
}

// deleteRule normalizes an omitted ON DELETE clause to the displayed default.
func deleteRule(value string) string {
	if strings.TrimSpace(value) == "" {
		return "NO ACTION"
	}
	return strings.ToUpper(value)
}

// ternary selects one of two strings without introducing a dependency on helpers.
func ternary(condition bool, yes, no string) string {
	if condition {
		return yes
	}
	return no
}

// titleCase makes auxiliary-object headings readable without changing identifiers.
func titleCase(value string) string {
	words := strings.Fields(strings.ToLower(value))
	for i, word := range words {
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}

// countNoun formats a count with a singular or plural noun.
func countNoun(count int, noun string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", count, noun)
}

// writeOutputsAtomically stages each output beside its destination and renames
// only after the complete byte stream has been written and closed.
func writeOutputsAtomically(outputs map[string][]byte) error {
	paths := make([]string, 0, len(outputs))
	for path := range outputs {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	temps := map[string]string{}
	cleanup := func() {
		// Temporary files are best-effort cleanup; the original destinations are
		// never removed before their replacement is ready.
		for _, temp := range temps {
			_ = os.Remove(temp)
		}
	}
	for _, path := range paths {
		temp, err := os.CreateTemp(filepath.Dir(path), ".dbs-schema-*")
		if err != nil {
			cleanup()
			return err
		}
		tempName := temp.Name()
		temps[path] = tempName
		if _, err = temp.Write(outputs[path]); err == nil {
			err = temp.Chmod(0o644)
		}
		if closeErr := temp.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			cleanup()
			return err
		}
	}
	for _, path := range paths {
		if err := os.Rename(temps[path], path); err != nil {
			cleanup()
			return err
		}
		delete(temps, path)
	}
	return nil
}

// mermaidID converts an SQL identifier into a stable Mermaid node identifier.
func mermaidID(value string) string {
	var b strings.Builder
	b.WriteString("n_")
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// mermaidText escapes characters that have structural meaning in Mermaid labels.
func mermaidText(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "\"", "'"), "\n", " ")
}

// md escapes table-cell separators in generated Markdown.
func md(value string) string { return strings.ReplaceAll(value, "|", "\\|") }
