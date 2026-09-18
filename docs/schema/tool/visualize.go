package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// nonERTypeCharacter identifies SQL type characters Mermaid ER identifiers reject.
var nonERTypeCharacter = regexp.MustCompile(`[^A-Za-z0-9_-]+`)

// selectedVisualizations returns modes in stable file-generation order.
func selectedVisualizations(selected map[string]bool) []string {
	var out []string
	for _, name := range []string{"er", "domains", "svg"} {
		if selected[name] {
			out = append(out, name)
		}
	}
	return out
}

// mermaidDirective returns the Mermaid initialization block for the requested density.
func mermaidDirective(layout string) string {
	if layout == "standard" {
		return `%%{init: {"flowchart": {"nodeSpacing": 12, "rankSpacing": 22, "curve": "linear", "padding": 4}, "themeVariables": {"fontSize": "12px"}}}%%`
	}
	return `%%{init: {"flowchart": {"nodeSpacing": 4, "rankSpacing": 8, "curve": "linear", "padding": 2}, "themeVariables": {"fontSize": "11px"}}}%%`
}

// domainDirection keeps domain maps top-to-bottom for readable GitHub rendering.
func domainDirection(group string) string {
	return "TB"
}

// renderLegend emits the shared notation, relationship, marker, and color legend.
func renderLegend(layout string) string {
	var b strings.Builder
	b.WriteString("## How to read this atlas\n\n")
	b.WriteString("A visual table is a labelled container; each inner box is a column. An arrow starts at the referencing (child) column and ends at the referenced key column. Arrow labels are foreign-key constraint names, not key names. To resolve a label such as `DS_BK`, search the `Foreign-key registry` section for its source and referenced columns, delete rule, referenced key, and source index; the same constraint is repeated in the affected table's `Table dictionary` entry under `Keys and constraints` and `Relationships`. Detailed Mermaid maps format labels as `CONSTRAINT · ACTION`, ER maps as `CONSTRAINT / ACTION`, and SVG maps show the same two values on separate lines. In the whole-schema overview, `N FKs` aggregates N distinct foreign keys between the same two tables.\n\n")
	b.WriteString("```mermaid\n" + mermaidDirective(layout) + "\nflowchart LR\n")
	b.WriteString("  subgraph CHILD[\"CHILD_TABLE · PK PK_CHILD · +2 folded\"]\n    direction TB\n    CID[\"ID<br/>INTEGER · PK · NN\"]\n    PID[\"PARENT_ID<br/>INTEGER · FK · NN\"]\n  end\n")
	b.WriteString("  subgraph PARENT[\"PARENT_TABLE · PK PK_PARENT\"]\n    direction TB\n    TID[\"ID<br/>INTEGER · PK · NN\"]\n  end\n")
	b.WriteString("  PID -->|\"FK_CHILD_PARENT · CASCADE\"| TID\n")
	b.WriteString("  style CHILD fill:#E6F5ED,stroke:#17734D,color:#1f2937\n  style PARENT fill:#E9F2FC,stroke:#245FA9,color:#1f2937\n```\n\n")
	b.WriteString("| Marker | Meaning |\n|---|---|\n")
	b.WriteString("| `PK` | Primary-key column |\n| `FK` | Foreign-key source column |\n| `UK` | Column participating in a unique constraint (`UQ` in DDL) |\n| `NN` | `NOT NULL` |\n| `IDX` | Explicit index (distinct from the referenced PK/UK) |\n| `IOT` | Oracle index-organized table |\n| `DDL` | Data Definition Language source script |\n")
	b.WriteString("\nDelete actions: `CASCADE` deletes dependent (child) rows when the referenced (parent) row is deleted; `SET NULL` clears the child FK value; `NO ACTION` means the DDL has no `ON DELETE` clause, so no child rows are automatically changed and a parent deletion can be rejected while dependents exist.\n\n")
	b.WriteString("Functional colors: 🟩 core data · 🟦 classification/lookup · 🟪 parentage · 🟧 processing configuration · 🔷 migration/instance metadata · ⬜ other operational tables.\n\n")
	b.WriteString("Database objects: **tables** store rows; **constraints** enforce keys/checks; **indexes** provide access paths; **sequences** generate numeric identifiers; **roles** group privileges; **grants** assign privileges.\n\n")
	return b.String()
}

// renderFullMap is the legacy all-column flowchart retained for compatibility.
func renderFullMap(schema *Schema, layout string) string {
	return renderFlowMap(schema, schema.Tables, schema.ForeignKeys, true, "TB", layout)
}

// renderKeyMap is the legacy key-focused flowchart retained for compatibility.
func renderKeyMap(schema *Schema, layout string) string {
	return renderFlowMap(schema, schema.Tables, schema.ForeignKeys, false, "TB", layout)
}

// renderFlowMap builds a column-level Mermaid flowchart for the supplied tables.
func renderFlowMap(schema *Schema, tables []Table, fks []ForeignKey, allColumns bool, direction, layout string) string {
	var b strings.Builder
	b.WriteString("```mermaid\n" + mermaidDirective(layout) + "\nflowchart " + direction + "\n")
	var orderedSubgraphs []string
	for _, table := range tables {
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

// renderERDiagram renders one functional area's complete Mermaid ER diagram.
func renderERDiagram(schema *Schema, group schemaGroup, layout string) string {
	var b strings.Builder
	members := tablesInGroup(schema, group.Key)
	if len(members) == 0 {
		return "No tables in this functional area."
	}
	fks := foreignKeysForGroup(schema.ForeignKeys, group.Key)
	tableSet := map[string]bool{}
	for _, table := range members {
		tableSet[canonicalName(table.Name)] = true
	}
	for _, fk := range fks {
		tableSet[canonicalName(fk.SourceTable)] = true
		tableSet[canonicalName(fk.TargetTable)] = true
	}
	b.WriteString("```mermaid\n")
	b.WriteString(`%%{init: {"themeVariables": {"fontSize": "11px"}}}%%` + "\n")
	b.WriteString("erDiagram\n")
	outgoing := map[string][]ForeignKey{}
	for _, fk := range fks {
		outgoing[canonicalName(fk.SourceTable)] = append(outgoing[canonicalName(fk.SourceTable)], fk)
	}
	for _, table := range schema.Tables {
		if !tableSet[canonicalName(table.Name)] {
			continue
		}
		fmt.Fprintf(&b, "  %s {\n", table.Name)
		for _, column := range table.Columns {
			var markers []string
			if containsName(primaryKey(table).Columns, column.Name) {
				markers = append(markers, "PK")
			}
			if isForeignKeyColumn(outgoing[canonicalName(table.Name)], column.Name) {
				markers = append(markers, "FK")
			}
			if uniqueColumn(table, column.Name) {
				markers = append(markers, "UK")
			}
			marker := ""
			if len(markers) != 0 {
				marker = " " + strings.Join(markers, ", ")
			}
			fmt.Fprintf(&b, "    %s %s%s\n", erType(column.Type), column.Name, marker)
		}
		b.WriteString("  }\n")
	}
	for _, fk := range fks {
		label := fk.Constraint + " / " + deleteRule(fk.OnDelete)
		fmt.Fprintf(&b, "  %s ||--o{ %s : \"%s\"\n", fk.TargetTable, fk.SourceTable, mermaidText(label))
	}
	b.WriteString("```")
	return b.String()
}

// renderERWholeDiagram renders one schema-wide Mermaid ER diagram for the mode's
// dedicated Whole-schema relations subsection.
func renderERWholeDiagram(schema *Schema, layout string) string {
	var b strings.Builder
	b.WriteString("```mermaid\n")
	b.WriteString(`%%{init: {"themeVariables": {"fontSize": "11px"}}}%%` + "\n")
	b.WriteString("erDiagram\n")
	outgoing := map[string][]ForeignKey{}
	for _, fk := range schema.ForeignKeys {
		outgoing[canonicalName(fk.SourceTable)] = append(outgoing[canonicalName(fk.SourceTable)], fk)
	}
	for _, table := range schema.Tables {
		fmt.Fprintf(&b, "  %s {\n", table.Name)
		for _, column := range table.Columns {
			var markers []string
			if containsName(primaryKey(table).Columns, column.Name) {
				markers = append(markers, "PK")
			}
			if isForeignKeyColumn(outgoing[canonicalName(table.Name)], column.Name) {
				markers = append(markers, "FK")
			}
			if uniqueColumn(table, column.Name) {
				markers = append(markers, "UK")
			}
			marker := ""
			if len(markers) != 0 {
				marker = " " + strings.Join(markers, ", ")
			}
			fmt.Fprintf(&b, "    %s %s%s\n", erType(column.Type), column.Name, marker)
		}
		b.WriteString("  }\n")
	}
	for _, fk := range schema.ForeignKeys {
		label := fk.Constraint + " / " + deleteRule(fk.OnDelete)
		fmt.Fprintf(&b, "  %s ||--o{ %s : \"%s\"\n", fk.TargetTable, fk.SourceTable, mermaidText(label))
	}
	b.WriteString("```")
	return b.String()
}

// mermaidTableHeader summarizes a table and reports folded columns.
func mermaidTableHeader(table Table, columns []Column) string {
	header := fmt.Sprintf("%s · PK %s · %d columns · %d shown", table.Name, primaryKey(table).Name, len(table.Columns), len(columns))
	if folded := len(table.Columns) - len(columns); folded > 0 {
		header += fmt.Sprintf(" · +%d folded", folded)
	}
	return header
}

// writeMermaidVerticalOrder adds invisible ordering edges to stabilize layout.
func writeMermaidVerticalOrder(b *strings.Builder, ids []string) {
	for index := 1; index < len(ids); index++ {
		fmt.Fprintf(b, "  %s ~~~ %s\n", ids[index-1], ids[index])
	}
}

// erType converts SQL type text into a Mermaid ER-safe type token.
func erType(value string) string {
	value = nonERTypeCharacter.ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "TYPE"
	}
	if value[0] >= '0' && value[0] <= '9' {
		return "T_" + value
	}
	return value
}

// aggregateOverviewEdges is kept separate so every compact overview uses a
// stable edge order and replaces parallel table-pair edges with a short count.
func aggregateOverviewEdges(fks []ForeignKey) []string {
	counts := map[string]int{}
	labels := map[string]string{}
	for _, fk := range fks {
		key := fk.SourceTable + "\x00" + fk.TargetTable
		counts[key]++
		labels[key] = fk.Constraint
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out []string
	for _, key := range keys {
		parts := strings.Split(key, "\x00")
		label := labels[key]
		if counts[key] > 1 {
			label = fmt.Sprintf("%d FKs", counts[key])
		}
		out = append(out, fmt.Sprintf("  %s -->|\"%s\"| %s", mermaidID("table_"+parts[0]), label, mermaidID("table_"+parts[1])))
	}
	return out
}
