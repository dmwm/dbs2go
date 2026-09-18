package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// main parses command-line options, builds the validated model, and atomically
// writes the requested JSON, Markdown, and optional native SVG artifacts.
func main() {
	var ddlPath, dialect, emit, outBase, svgOutBase, visualizations, layout string
	flag.StringVar(&ddlPath, "ddl", "", "DDL file to parse")
	flag.StringVar(&dialect, "dialect", "oracle", "SQL dialect: oracle or mysql")
	flag.StringVar(&emit, "emit", "json,markdown", "comma-separated outputs: json,markdown")
	flag.StringVar(&outBase, "out", "", "output basename")
	flag.StringVar(&svgOutBase, "svg-out", "", "SVG asset basename (default: -out value)")
	flag.StringVar(&visualizations, "visualizations", "domains", "comma-separated views: er,domains,svg,all")
	flag.StringVar(&layout, "layout", "compact", "diagram spacing: compact or standard")
	flag.Parse()

	if ddlPath == "" {
		fatalf("-ddl is required")
	}
	formats, err := parseEmitters(emit)
	if err != nil {
		fatalf("%v", err)
	}
	views, err := parseVisualizations(visualizations)
	if err != nil {
		fatalf("%v", err)
	}
	layout = strings.ToLower(strings.TrimSpace(layout))
	if layout != "compact" && layout != "standard" {
		fatalf("unknown layout %q (supported: compact, standard)", layout)
	}
	input, err := os.ReadFile(ddlPath)
	if err != nil {
		fatalf("read %s: %v", ddlPath, err)
	}
	schema, err := parseSchema(filepath.Base(ddlPath), input, dialect)
	if err != nil {
		fatalf("%v", err)
	}
	if outBase == "" {
		outBase = ddlPath + ".schema"
	}
	outBase = strings.TrimSuffix(outBase, ".md")
	if svgOutBase == "" {
		svgOutBase = outBase
	}
	// Keep all artifacts in memory until parsing and rendering succeed; the
	// atomic writer below prevents a failed run from leaving partial outputs.
	outputs := make(map[string][]byte, len(formats)+len(schemaGroups))
	svgReferences := map[string]string{}
	if views["svg"] {
		svgs, err := renderNativeSVGs(schema, layout)
		if err != nil {
			fatalf("render native SVG: %v", err)
		}
		for key, data := range svgs {
			path := svgOutBase + "." + key + ".svg"
			outputs[path] = data
			reference, relErr := filepath.Rel(filepath.Dir(outBase+".md"), path)
			if relErr != nil {
				fatalf("resolve SVG reference: %v", relErr)
			}
			svgReferences[key] = filepath.ToSlash(reference)
		}
	}
	for _, format := range formats {
		switch format {
		case "json":
			outputs[outBase+".json"], err = renderJSON(schema)
		case "markdown":
			markdownBase := strings.TrimSuffix(outBase, ".md")
			for _, view := range selectedVisualizations(views) {
				outputs[markdownBase+"."+view+".md"] = []byte(renderMarkdownWithOptions(schema, RenderOptions{Visualizations: map[string]bool{view: true}, Layout: layout, SVGReferences: svgReferences}))
			}
		}
		if err != nil {
			fatalf("render %s: %v", format, err)
		}
	}
	if err := writeOutputsAtomically(outputs); err != nil {
		fatalf("write output: %v", err)
	}
	fmt.Printf("tables=%d columns=%d foreign_keys=%d auxiliary=%d\n", len(schema.Tables), columnCount(schema), len(schema.ForeignKeys), len(schema.Auxiliary))
}

// parseVisualizations validates and canonicalizes the visualization mode list.
func parseVisualizations(value string) (map[string]bool, error) {
	allowed := map[string]bool{"er": true, "domains": true, "svg": true}
	out := map[string]bool{}
	for _, item := range strings.Split(value, ",") {
		item = strings.ToLower(strings.TrimSpace(item))
		if item == "" {
			continue
		}
		if item == "all" {
			for name := range allowed {
				out[name] = true
			}
			continue
		}
		if !allowed[item] {
			return nil, fmt.Errorf("unknown visualization %q (supported: er, domains, svg, all)", item)
		}
		out[item] = true
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one visualization is required")
	}
	return out, nil
}

// parseEmitters validates output formats, accepting mermaid as a Markdown alias.
func parseEmitters(value string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, item := range strings.Split(value, ",") {
		item = strings.ToLower(strings.TrimSpace(item))
		if item == "mermaid" {
			item = "markdown"
		}
		if item != "json" && item != "markdown" {
			return nil, fmt.Errorf("unknown emitter %q (supported: json, markdown)", item)
		}
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one emitter is required")
	}
	return out, nil
}

// columnCount returns the total number of native columns in the schema.
func columnCount(schema *Schema) int {
	n := 0
	for _, table := range schema.Tables {
		n += len(table.Columns)
	}
	return n
}

// fatalf reports a user-facing failure and exits with the tool's fatal status.
func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "dbs-schema: "+format+"\n", args...)
	os.Exit(2)
}
