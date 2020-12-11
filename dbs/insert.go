package dbs

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/vkuznet/dbs2go/utils"
)

// InsertData API
func InsertData(tmpl string, values Record) error {
	// get SQL statement from static area
	stm := getSQL(tmpl)
	var vals []interface{}
	var args []string
	for k, v := range values {
		if !strings.Contains(strings.ToLower(stm), k) {
			msg := fmt.Sprintf("unable to find column '%s' in %s", k, stm)
			return errors.New(msg)
		}
		vals = append(vals, v)
		args = append(args, "?")
	}
	stm = fmt.Sprintf("%s VALUES (%s)", stm, strings.Join(args, ","))
	if utils.VERBOSE > 0 {
		log.Println("InsertData", stm, vals, values)
	}
	// use generic query API to fetch the results from DB
	return insert(stm, vals)
}
