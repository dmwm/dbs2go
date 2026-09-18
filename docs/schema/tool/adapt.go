package main

import (
	"fmt"
	"strings"
)

// adaptCreateTable normalizes dialect constructs that GoSQLX cannot parse while
// returning metadata needed to restore their meaning in the validated model.
func adaptCreateTable(sqlText, dialect string) (string, map[string][]string, string, bool, error) {
	named := map[string][]string{}
	open := indexOutsideQuotes(sqlText, '(')
	if open < 0 {
		return "", nil, "", false, fmt.Errorf("CREATE TABLE has no opening parenthesis")
	}
	close, err := matchingParen(sqlText, open)
	if err != nil {
		return "", nil, "", false, err
	}
	suffix := strings.TrimSpace(sqlText[close+1:])
	organization := ""
	if suffix != "" {
		// Oracle's index-organized-table suffix is retained as model metadata;
		// MySQL ENGINE clauses are passed through to the parser unchanged.
		if dialect == "oracle" && strings.EqualFold(strings.Join(strings.Fields(suffix), " "), "ORGANIZATION INDEX") {
			organization = "INDEX"
		} else if dialect != "mysql" || !strings.HasPrefix(strings.ToUpper(strings.Join(strings.Fields(suffix), " ")), "ENGINE =") {
			return "", nil, "", false, fmt.Errorf("unrecognized CREATE TABLE suffix %q", suffix)
		}
	}

	definitions, err := splitTopLevel(sqlText[open+1:close], ',')
	if err != nil {
		return "", nil, "", false, err
	}
	adapted := false
	for i, definition := range definitions {
		columnName := leadingIdentifier(definition)
		if columnName == "" || isTableConstraint(columnName) {
			continue
		}
		matches := namedNotNullRE.FindAllStringSubmatch(definition, -1)
		for _, match := range matches {
			named[canonicalName(columnName)] = append(named[canonicalName(columnName)], cleanName(match[1]))
		}
		if len(matches) != 0 {
			// GoSQLX accepts NOT NULL but loses a named inline constraint, so
			// replace only the syntax and preserve each name in named.
			definitions[i] = namedNotNullRE.ReplaceAllString(definition, "NOT NULL")
			adapted = true
		}
	}
	adaptedSQL := sqlText[:open+1] + strings.Join(definitions, ",") + ")"
	if dialect == "mysql" && suffix != "" {
		adaptedSQL += " " + suffix
	}
	return adaptedSQL, named, organization, adapted, nil
}

// parseExpressionIndex parses a CREATE INDEX fallback containing an expression
// that GoSQLX rejected, preserving the expression instead of dropping the index.
func parseExpressionIndex(sqlText string, line int) (Index, string, error) {
	match := createIndexRE.FindStringSubmatchIndex(sqlText)
	if match == nil {
		return Index{}, "", fmt.Errorf("not a CREATE INDEX statement")
	}
	unique := strings.TrimSpace(sqlText[match[2]:match[3]]) != ""
	name := cleanName(sqlText[match[4]:match[5]])
	table := cleanName(sqlText[match[6]:match[7]])
	open := match[1] - 1
	close, err := matchingParen(sqlText, open)
	if err != nil {
		return Index{}, "", err
	}
	if strings.TrimSpace(sqlText[close+1:]) != "" {
		return Index{}, "", fmt.Errorf("unsupported CREATE INDEX suffix %q", strings.TrimSpace(sqlText[close+1:]))
	}
	parts, err := splitTopLevel(sqlText[open+1:close], ',')
	if err != nil {
		return Index{}, "", err
	}
	idx := Index{Name: name, Unique: unique, SourceLine: line}
	hasExpression := false
	for _, part := range parts {
		if simple := simpleIndexRE.FindStringSubmatch(part); simple != nil {
			idx.Parts = append(idx.Parts, IndexPart{Column: cleanName(simple[1]), Direction: strings.ToUpper(simple[2])})
		} else {
			expression := strings.TrimSpace(part)
			if expression == "" {
				return Index{}, "", fmt.Errorf("empty index expression")
			}
			idx.Parts = append(idx.Parts, IndexPart{Expression: expression})
			hasExpression = true
		}
	}
	if !hasExpression {
		return Index{}, "", fmt.Errorf("GoSQLX failed a simple index; compatibility adapter refused it")
	}
	return idx, table, nil
}

// indexOutsideQuotes returns the first wanted byte that is not inside a quote.
func indexOutsideQuotes(value string, wanted byte) int {
	var quote byte
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(value) && value[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		if ch == wanted {
			return i
		}
	}
	return -1
}

// matchingParen finds the closing parenthesis paired with open, quote-aware.
func matchingParen(value string, open int) (int, error) {
	depth := 0
	var quote byte
	for i := open; i < len(value); i++ {
		ch := value[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(value) && value[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			} else if ch == '\\' && i+1 < len(value) {
				i++
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return 0, fmt.Errorf("unbalanced parentheses")
}

// splitTopLevel splits a definition list without treating quoted or nested
// separators as delimiters.
func splitTopLevel(value string, separator byte) ([]string, error) {
	var out []string
	start, depth := 0, 0
	var quote byte
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(value) && value[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			} else if ch == '\\' && i+1 < len(value) {
				i++
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced parentheses")
			}
		default:
			if ch == separator && depth == 0 {
				out = append(out, value[start:i])
				start = i + 1
			}
		}
	}
	if quote != 0 || depth != 0 {
		return nil, fmt.Errorf("unbalanced expression")
	}
	out = append(out, value[start:])
	return out, nil
}

// leadingIdentifier extracts the first SQL identifier from a table definition.
func leadingIdentifier(definition string) string {
	definition = strings.TrimSpace(definition)
	if definition == "" {
		return ""
	}
	if definition[0] == '"' || definition[0] == '`' {
		quote := definition[0]
		if end := strings.IndexByte(definition[1:], quote); end >= 0 {
			return definition[:end+2]
		}
		return ""
	}
	for i, ch := range definition {
		if isSpace(ch) || ch == '(' {
			return definition[:i]
		}
	}
	return definition
}

// isTableConstraint reports whether an item begins with a table-level constraint.
func isTableConstraint(identifier string) bool {
	switch canonicalName(identifier) {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK":
		return true
	default:
		return false
	}
}
