package main

import (
	"fmt"
	"html"
	"sort"
	"strings"
)

// svgBox stores the rendered geometry and column ports for one table card.
type svgBox struct {
	Table   Table
	Columns []Column
	X, Y    float64
	Width   float64
	Height  float64
	Header  float64
	Row     float64
	Rows    map[string]float64
}

// renderNativeSVGs renders one whole-schema asset plus one asset per functional area.
func renderNativeSVGs(schema *Schema, layout string) (map[string][]byte, error) {
	out := map[string][]byte{}
	whole := schemaGroup{"whole", "Whole-schema relations", "All DBS tables and relationships.", "#E8EEF5", "#40566F"}
	// The whole asset is separate from the five group assets so each Markdown
	// mode can embed one graph for its Whole-schema relations subsection.
	wholeData, err := renderNativeSVG(schema, whole, schema.Tables, schema.ForeignKeys, layout)
	if err != nil {
		return nil, fmt.Errorf("whole: %w", err)
	}
	out["whole"] = wholeData
	for _, group := range schemaGroups {
		members := tablesInGroup(schema, group.Key)
		if len(members) == 0 {
			continue
		}
		fks := foreignKeysForGroup(schema.ForeignKeys, group.Key)
		data, err := renderNativeSVG(schema, group, members, fks, layout)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", group.Key, err)
		}
		out[group.Key] = data
	}
	return out, nil
}

// renderNativeSVG lays out table cards, paints exact FK paths, and serializes a
// self-contained SVG without invoking Graphviz or another external renderer.
func renderNativeSVG(schema *Schema, group schemaGroup, members []Table, fks []ForeignKey, layout string) ([]byte, error) {
	tableSet := map[string]bool{}
	for _, table := range members {
		tableSet[canonicalName(table.Name)] = true
	}
	for _, fk := range fks {
		tableSet[canonicalName(fk.SourceTable)] = true
		tableSet[canonicalName(fk.TargetTable)] = true
	}
	var tables []Table
	for _, table := range schema.Tables {
		if tableSet[canonicalName(table.Name)] {
			tables = append(tables, table)
		}
	}

	levels := svgLevels(tables, fks)
	maxLevel := 0
	for _, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
	}
	byLevel := make(map[int][]Table)
	for _, table := range tables {
		byLevel[levels[canonicalName(table.Name)]] = append(byLevel[levels[canonicalName(table.Name)]], table)
	}
	for level := range byLevel {
		sort.Slice(byLevel[level], func(i, j int) bool { return byLevel[level][i].Name < byLevel[level][j].Name })
	}

	width, row, header, gapX, gapY := 260.0, 23.0, 46.0, 76.0, 14.0
	if layout == "standard" {
		// Standard mode increases card and inter-column spacing for inspection;
		// compact mode is the GitHub-friendly default.
		width, row, header, gapX, gapY = 320, 28, 50, 130, 28
	}
	margin, top := 32.0, 142.0
	boxes := map[string]*svgBox{}
	maxBottom := top
	for level := 0; level <= maxLevel; level++ {
		// Referenced tables are placed to the right of their dependents; tables
		// at the same level are stacked vertically in stable name order.
		x := margin + float64(maxLevel-level)*(width+gapX)
		y := top
		for _, table := range byLevel[level] {
			box := newSVGBox(table, mapColumnsFor(table, fks, false), x, y, width, header, row)
			boxes[canonicalName(table.Name)] = box
			y += box.Height + gapY
		}
		if y-gapY > maxBottom {
			maxBottom = y - gapY
		}
	}
	canvasWidth := margin*2 + float64(maxLevel+1)*width + float64(maxLevel)*gapX
	if canvasWidth < 680 {
		canvasWidth = 680
	}
	canvasHeight := maxBottom + 28

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %.0f %.0f" width="%.0f" height="%.0f" role="img" aria-labelledby="title desc">`, canvasWidth, canvasHeight, canvasWidth, canvasHeight)
	b.WriteString("\n<title id=\"title\">DBS relational map — " + xmlText(group.Title) + "</title>\n")
	b.WriteString("<desc id=\"desc\">DBS tables with exact foreign-key column connections, generated directly by the Go schema tool.</desc>\n")
	b.WriteString(`<defs><marker id="fk-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="` + group.Stroke + `"/></marker></defs>` + "\n")
	b.WriteString(`<rect width="100%" height="100%" fill="#f8fafc"/>` + "\n")
	fmt.Fprintf(&b, `<text x="%.0f" y="38" font-family="system-ui, sans-serif" font-size="22" font-weight="700" fill="#172b4d">%s</text>`+"\n", margin, xmlText(group.Title))
	fmt.Fprintf(&b, `<text x="%.0f" y="62" font-family="system-ui, sans-serif" font-size="12" fill="#52657a">%s</text>`+"\n", margin, xmlText(group.Description))
	fmt.Fprintf(&b, `<text x="%.0f" y="78" font-family="system-ui, sans-serif" font-size="11" fill="#52657a">Exact arrows run from FK source columns to referenced PK/UQ columns.</text>`+"\n", margin)
	legendNames := []string{"Core", "Lookup", "Parentage", "Config", "Operations", "Other"}
	for index, legendGroup := range schemaGroups {
		x := margin + float64(index)*110
		fmt.Fprintf(&b, `<rect x="%.0f" y="92" width="12" height="12" rx="2" fill="%s" stroke="%s"/><text x="%.0f" y="103" font-family="system-ui, sans-serif" font-size="10" fill="#334155">%s</text>`+"\n", x, legendGroup.Fill, legendGroup.Stroke, x+18, legendNames[index])
	}
	fmt.Fprintf(&b, `<text x="%.0f" y="124" font-family="ui-monospace, monospace" font-size="10" fill="#52657a">PK primary key · FK foreign key · UQ unique · NN not null · +N folded columns are in the dictionary</text>`+"\n", margin)

	// Edges are painted first so table cards and row ports remain legible.
	// Pair offsets keep parallel relationships distinguishable without changing endpoints.
	pairCount := map[string]int{}
	for _, fk := range fks {
		for index, sourceColumn := range fk.SourceColumns {
			source := boxes[canonicalName(fk.SourceTable)]
			target := boxes[canonicalName(fk.TargetTable)]
			if source == nil || target == nil {
				return nil, fmt.Errorf("missing SVG table for foreign key %s", fk.Constraint)
			}
			sy, sourceOK := source.Rows[canonicalName(sourceColumn)]
			ty, targetOK := target.Rows[canonicalName(fk.TargetColumns[index])]
			if !sourceOK || !targetOK {
				return nil, fmt.Errorf("missing SVG column port for foreign key %s", fk.Constraint)
			}
			sx, tx := source.X, target.X+target.Width
			pair := canonicalName(fk.SourceTable) + "\x00" + canonicalName(fk.TargetTable)
			offset := float64(pairCount[pair]%5-2) * 5
			pairCount[pair]++
			mid := (sx+tx)/2 + offset
			if sx <= tx {
				sx = source.X + source.Width
				tx = target.X
				mid = (sx+tx)/2 + offset
			}
			fmt.Fprintf(&b, `<path d="M %.1f %.1f H %.1f V %.1f H %.1f" fill="none" stroke="%s" stroke-width="1.6" opacity="0.82" marker-end="url(#fk-arrow)"/>`+"\n", sx, sy, mid, ty, tx, group.Stroke)
			if index == 0 {
				renderSVGEdgeLabel(&b, (sx+tx)/2-34, sy-10, group.Stroke, fk.Constraint, deleteRule(fk.OnDelete))
			}
		}
	}

	for _, table := range tables {
		box := boxes[canonicalName(table.Name)]
		style := groupForTable(table.Name)
		renderSVGTable(&b, box, style, fks)
	}
	b.WriteString("</svg>\n")
	return []byte(b.String()), nil
}

// newSVGBox computes a table card's dimensions and row-port coordinates.
func newSVGBox(table Table, columns []Column, x, y, width, header, row float64) *svgBox {
	box := &svgBox{Table: table, Columns: columns, X: x, Y: y, Width: width, Header: header, Row: row, Rows: map[string]float64{}}
	box.Height = header + float64(len(columns))*row
	for index, column := range columns {
		box.Rows[canonicalName(column.Name)] = y + header + (float64(index)+0.5)*row
	}
	return box
}

// renderSVGEdgeLabel writes the two-line FK constraint/delete-rule label.
func renderSVGEdgeLabel(b *strings.Builder, x, y float64, color, constraint, action string) {
	fmt.Fprintf(b, `<rect x="%.1f" y="%.1f" width="68" height="20" rx="3" fill="#ffffff" opacity="0.94"/>`+"\n", x, y)
	fmt.Fprintf(b, `<text x="%.1f" y="%.1f" text-anchor="middle" font-family="ui-monospace, monospace" font-size="7.5" font-weight="700" fill="%s"><tspan x="%.1f">%s</tspan><tspan x="%.1f" dy="7.5" font-size="7" font-weight="500">%s</tspan></text>`+"\n", x+34, y+7.5, color, x+34, xmlText(constraint), x+34, xmlText(action))
}

// svgLevels assigns dependency levels used to place referenced tables toward
// the right and dependent tables toward the left of the SVG.
func svgLevels(tables []Table, fks []ForeignKey) map[string]int {
	levels := map[string]int{}
	allowed := map[string]bool{}
	for _, table := range tables {
		allowed[canonicalName(table.Name)] = true
	}
	// The bounded relaxation handles acyclic dependencies and terminates safely
	// even when the source schema contains a cycle.
	for iteration := 0; iteration < len(tables); iteration++ {
		changed := false
		for _, fk := range fks {
			source, target := canonicalName(fk.SourceTable), canonicalName(fk.TargetTable)
			if !allowed[source] || !allowed[target] || source == target {
				continue
			}
			candidate := levels[target] + 1
			if candidate > levels[source] {
				levels[source] = candidate
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return levels
}

// renderSVGTable draws one card, folded-column summary, badges, and row ports.
func renderSVGTable(b *strings.Builder, box *svgBox, group schemaGroup, fks []ForeignKey) {
	badgeOffset, nameOffset := 6.0, 58.0
	badgeSize, nameSize, typeSize := 7.5, 9.0, 7.5
	if box.Width >= 300 {
		badgeOffset, nameOffset = 8, 70
		badgeSize, nameSize, typeSize = 9, 10.5, 9
	}
	fmt.Fprintf(b, `<g id="table-%s">`+"\n", xmlID(box.Table.Name))
	fmt.Fprintf(b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="7" fill="#ffffff" stroke="%s" stroke-width="1.4"/>`+"\n", box.X, box.Y, box.Width, box.Height, group.Stroke)
	fmt.Fprintf(b, `<path d="M %.1f %.1f h %.1f v %.1f h -%.1f z" fill="%s"/>`+"\n", box.X, box.Y+7, box.Width, box.Header-7, box.Width, group.Fill)
	fmt.Fprintf(b, `<text x="%.1f" y="%.1f" font-family="ui-monospace, monospace" font-size="13" font-weight="700" fill="%s">%s</text>`+"\n", box.X+10, box.Y+19, group.Stroke, xmlText(box.Table.Name))
	pk := primaryKey(box.Table)
	folded := len(box.Table.Columns) - len(box.Columns)
	subtitle := "PK " + pk.Name
	if folded > 0 {
		subtitle += fmt.Sprintf(" · +%d folded", folded)
	}
	fmt.Fprintf(b, `<text x="%.1f" y="%.1f" font-family="system-ui, sans-serif" font-size="9.5" fill="#53657a">%s</text>`+"\n", box.X+10, box.Y+36, xmlText(subtitle))
	for index, column := range box.Columns {
		y := box.Y + box.Header + float64(index)*box.Row
		fill := "#ffffff"
		if index%2 == 1 {
			fill = "#f8fafc"
		}
		fmt.Fprintf(b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s" stroke="#e5eaf0" stroke-width="0.6"/>`+"\n", box.X, y, box.Width, box.Row, fill)
		badges := svgBadges(box.Table, column, fks)
		fmt.Fprintf(b, `<text x="%.1f" y="%.1f" font-family="ui-monospace, monospace" font-size="%.1f" font-weight="700" fill="#64748b">%s</text>`+"\n", box.X+badgeOffset, y+15, badgeSize, xmlText(strings.Join(badges, " ")))
		fmt.Fprintf(b, `<text x="%.1f" y="%.1f" font-family="ui-monospace, monospace" font-size="%.1f" font-weight="600" fill="#172b4d">%s</text>`+"\n", box.X+nameOffset, y+15, nameSize, xmlText(column.Name))
		fmt.Fprintf(b, `<text x="%.1f" y="%.1f" text-anchor="end" font-family="ui-monospace, monospace" font-size="%.1f" fill="#52657a">%s</text>`+"\n", box.X+box.Width-6, y+15, typeSize, xmlText(column.Type))
		center := y + box.Row/2
		fmt.Fprintf(b, `<circle cx="%.1f" cy="%.1f" r="2.5" fill="%s"/><circle cx="%.1f" cy="%.1f" r="2.5" fill="%s"/>`+"\n", box.X, center, group.Stroke, box.X+box.Width, center, group.Stroke)
	}
	b.WriteString("</g>\n")
}

// svgBadges returns the compact semantic markers displayed beside a column.
func svgBadges(table Table, column Column, fks []ForeignKey) []string {
	var badges []string
	if containsName(primaryKey(table).Columns, column.Name) {
		badges = append(badges, "PK")
	}
	for _, fk := range fks {
		if canonicalName(fk.SourceTable) == canonicalName(table.Name) && containsName(fk.SourceColumns, column.Name) {
			badges = appendOnce(badges, "FK")
		}
	}
	if uniqueColumn(table, column.Name) {
		badges = appendOnce(badges, "UQ")
	}
	if !column.Nullable {
		badges = appendOnce(badges, "NN")
	}
	return badges
}

// xmlText escapes user/schema text before placing it in SVG markup.
func xmlText(value string) string { return html.EscapeString(value) }

// xmlID converts a table name to an SVG-safe element identifier.
func xmlID(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	return b.String()
}
