package dbs

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

// helper function to generate token's SQL statement out of given values
func tokens(inputList []string) (string, []string) {
	var vals []string
	values := ""
	limit := 100
	for _, d := range inputList {
		if values == "" { // first time
			values = d
			continue
		}
		if len(values)+1+len(d) < limit {
			values += fmt.Sprintf(",%s", d)
		} else {
			vals = append(vals, values)
			values = d
		}
	}
	if len(vals) == 0 && values != "" {
		vals = append(vals, values)
	}

	stm := ""
	for i, _ := range vals {
		if i > 0 {
			stm += "\n UNION ALL \n"
		}
		t := fmt.Sprintf(":token%d", i)
		stm += fmt.Sprintf("SELECT REGEXP_SUBSTR(%s, '[^,]+', 1, LEVEL) token FROM DUAL ", t)
		stm += fmt.Sprintf("CONNECT BY LEVEL <= LENGTH(%s) - LENGTH(REPLACE(%s, ',', '')) + 1\n", t, t)
	}
	out := fmt.Sprintf("WITH TOKEN_GENERATOR AS(\n%s)", stm)
	return out, vals
}

// helper function to generate operator, value pair for given argument
func OperatorValue(arg string) (string, string) {
	op := "="
	val := arg
	if strings.Contains(arg, "*") {
		op = "like"
		val = strings.Replace(arg, "*", "%", -1)
	}
	return op, val
}

// ParseRuns parse run_num parameter and convert it to run list
func ParseRuns(runs []string) ([]string, error) {
	var out []string
	for _, v := range runs {
		if matched := intPattern.MatchString(v); matched {
			out = append(out, v)
		} else if matched := runRangePattern.MatchString(v); matched {
			out = append(out, v)
		} else if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
			runs := strings.Replace(v, "[", "", -1)
			runs = strings.Replace(runs, "]", "", -1)
			for _, r := range strings.Split(runs, ",") {
				run := strings.Trim(r, " ")
				log.Println("input", r)
				out = append(out, run)
			}
		} else {
			err := errors.New(fmt.Sprintf("invalid run input parameter %s", v))
			return out, err
		}
	}
	return out, nil
}

// CreateTokenGenerator creates a SQL token generator statement
// https://betteratoracle.com/posts/20-how-do-i-bind-a-variable-in-list
func CreateTokenGenerator(runs []string) (string, []string) {
	const limit = 4000 // oracle limit
	stm := "WITH TOKEN_GENERATOR AS ( "
	var tstm []string
	var vals []string
	for idx, chunk := range GetChunks(runs, limit) {
		t := fmt.Sprintf("token_%d", idx)
		s := fmt.Sprintf("SELECT REGEXP_SUBSTR(:%s, '[^,]+', 1, LEVEL) token ", t)
		s += "FROM DUAL"
		s += fmt.Sprintf("CONNECT BY LEVEL <= length(:%s) - length(REPLACE(:%s, ',', '')) + 1", t, t)
		tstm = append(tstm, s)
		vals = append(vals, chunk)
	}
	stm += strings.Join(tstm, " UNION ALL ")
	stm += " ) "
	return stm, vals
}

// helper function to get ORACLE chunks from provided list of values
func GetChunks(vals []string, limit int) []string {
	var chunks []string
	if len(vals) < limit {
		return []string{strings.Join(vals, ",")}
	}
	idx := 0
	exit := false
	for {
		end := idx + limit
		if end > len(vals) {
			end = len(vals)
			exit = true
		}
		chunk := strings.Join(vals[idx:end], ",")
		chunks = append(chunks, chunk)
		idx = end
		if exit {
			break
		}
	}
	return chunks
}
