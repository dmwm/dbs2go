# DBS schema visualizer

This is the canonical, standalone replacement for the experiments in the
parent directory. It parses DBS DDL into a validated schema model and emits
deterministic JSON plus GitHub-native Markdown/Mermaid atlases. Each selected
visualization is written to its own `../README.<option>.md`; the output needs
no HTML, GitHub Pages, or external renderer.

It uses GoSQLX v1.14.0 for the relational AST. Small, explicit compatibility
adapters preserve Oracle syntax GoSQLX cannot currently represent: named
column-level `NOT NULL` constraints, `ORGANIZATION INDEX`, and function-index
expressions. Comments and non-relational setup statements are handled by the
script layer. Unknown or unresolved relational statements are fatal; the tool
never presents a partial schema as complete.

## Run

From this directory:

```sh
go run . \
  -ddl ../../../static/schema/DDL/create-oracle-schema.sql \
  -dialect oracle \
  -out ../README \
  -svg-out ../generated/dbs-oracle \
  -emit markdown \
  -visualizations all \
  -layout compact

go run . \
  -ddl ../../../static/schema/DDL/create-oracle-schema.sql \
  -dialect oracle \
  -out ../generated/dbs-oracle \
  -emit json
```

`-visualizations all` writes one self-contained Markdown document per option:
`../README.er.md`, `../README.domains.md`, and `../README.svg.md`. Selecting a comma-separated
subset writes only those option files. Each document includes the shared
legend, architecture, whole-schema relation overview, foreign-key registry,
table dictionary, and auxiliary objects. The fixed Whole-schema relations
subsection and the five functional-area subsections under `## Relational maps`
are the only visualization content: the selected mode is embedded once in each
subsection.

For MySQL, use `create-mysql-schema.sql` and `-dialect mysql`.

`-visualizations` accepts any comma-separated combination of:

- `er`: compact Mermaid ER tables;
- `domains`: five smaller exact column-level maps;
- `svg`: one whole-schema plus five deterministic Go-native SVGs with fixed
  table cards and column ports;
- `all`: all three modes.

`-layout compact` is the default and applies reduced spacing to all flowchart
views and SVG assets. `-layout standard` is available for less dense output.
The SVG implementation is pure Go and does not invoke Graphviz or another
external renderer.

The Markdown also contains a shared legend, architecture view, grouped
foreign-key registry, collapsed table dictionaries, and non-table objects such
as sequences, roles, and grants. The JSON is the machine-readable source for
later API-to-database mapping work.

## Verify

```sh
go test ./...
go vet ./...
```

The integration tests parse both checked-in DBS DDL files and assert their full
table, column, and foreign-key counts. The root dbs2go Go module is not changed.
