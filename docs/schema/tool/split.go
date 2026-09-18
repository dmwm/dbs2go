package main

import (
	"fmt"
	"strings"
)

// splitStatements removes comments while preserving newlines (and therefore
// source locations), and splits only on semicolons outside quoted text.
func splitStatements(src string) ([]statement, error) {
	runes := []rune(src)
	var out []statement
	var b strings.Builder
	line, startLine := 1, 0
	var quote rune
	lineComment, blockComment := false, false

	flush := func() {
		sql := strings.TrimSpace(b.String())
		if sql != "" {
			out = append(out, statement{SQL: sql, Line: startLine})
		}
		b.Reset()
		startLine = 0
	}

	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		var next rune
		if i+1 < len(runes) {
			next = runes[i+1]
		}
		if lineComment {
			// Preserve newline characters from comments so diagnostic line numbers
			// continue to refer to the original DDL.
			if ch == '\n' {
				lineComment = false
				b.WriteRune(ch)
				line++
			}
			continue
		}
		if blockComment {
			if ch == '\n' {
				b.WriteRune(ch)
				line++
			}
			if ch == '*' && next == '/' {
				blockComment = false
				i++
			}
			continue
		}
		if quote != 0 {
			// Quoted semicolons and escaped/doubled quote characters belong to the
			// current statement rather than the outer scanner.
			b.WriteRune(ch)
			if ch == '\n' {
				line++
			}
			if ch == quote {
				if next == quote {
					b.WriteRune(next)
					i++
				} else {
					quote = 0
				}
			} else if ch == '\\' && next != 0 {
				b.WriteRune(next)
				i++
			}
			continue
		}
		if ch == '-' && next == '-' {
			lineComment = true
			i++
			continue
		}
		if ch == '#' {
			lineComment = true
			continue
		}
		if ch == '/' && next == '*' {
			blockComment = true
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			if startLine == 0 {
				startLine = line
			}
			quote = ch
			b.WriteRune(ch)
			continue
		}
		if ch == ';' {
			flush()
			continue
		}
		if startLine == 0 && !isSpace(ch) {
			startLine = line
		}
		b.WriteRune(ch)
		if ch == '\n' {
			line++
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quoted text at line %d", line)
	}
	if blockComment {
		return nil, fmt.Errorf("unterminated block comment at line %d", line)
	}
	flush()
	return out, nil
}

// isSpace identifies the SQL whitespace characters relevant to statement scanning.
func isSpace(r rune) bool { return r == ' ' || r == '\t' || r == '\r' || r == '\n' }
