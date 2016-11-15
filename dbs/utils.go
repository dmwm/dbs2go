package dbs

import (
	"fmt"
	"strings"
)

// helper function to generate token's SQL statement out of given datasets
func tokens(datasets []string) (string, []string) {
	var vals []string
	values := ""
	limit := 100
	for _, d := range datasets {
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
func opVal(arg string) (string, string) {
	op := "="
	val := arg
	if strings.Contains(arg, "*") {
		op = "like"
		val = strings.Replace(arg, "*", "%", -1)
	}
	return op, val
}
