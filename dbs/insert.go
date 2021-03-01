package dbs

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/vkuznet/dbs2go/utils"
)

// helper function to prepare insert statement with values
func StatementValues(tmpl string, values Record) (string, []interface{}, error) {
	// get SQL statement from static area
	stm := getSQL(tmpl)
	var vals []interface{}
	var args []string
	var params []string
	for k, v := range values {
		params = append(params, strings.ToUpper(k))
		vals = append(vals, v)
		args = append(args, "?")
	}
	stm = fmt.Sprintf("%s (%s) VALUES (%s)", stm, strings.Join(params, ","), strings.Join(args, ","))
	if utils.VERBOSE > 0 {
		log.Println("StatementValues", stm, vals)
	}
	return stm, vals, nil
}

// InsertValues API
func InsertValues(tmpl string, values Record) error {
	stm, vals, err := StatementValues(tmpl, values)
	if err != nil {
		return err
	}
	return insert(stm, vals)
}

// helper function to prepare insert statement with templated values
func StatementTemplateValues(tmpl string, args, values Record) (string, []interface{}, error) {
	var vals []interface{}
	stm, err := LoadTemplateSQL(tmpl, args)
	if err != nil {
		return "", vals, err
	}
	for k, v := range values {
		if !strings.Contains(strings.ToLower(stm), k) {
			msg := fmt.Sprintf("unable to find column '%s' in %s", k, stm)
			return "", vals, errors.New(msg)
		}
		// replace all bind names with ?
		stm = strings.Replace(stm, fmt.Sprintf(":%s", k), "?", -1)
		vals = append(vals, v)
	}
	if utils.VERBOSE > 0 {
		log.Println("InsertTemplateValues", stm, vals)
	}
	return stm, vals, nil
}

// InsertTemplateValues API
func InsertTemplateValues(tmpl string, args, values Record) error {
	stm, vals, err := StatementTemplateValues(tmpl, args, values)
	if err != nil {
		return err
	}
	return insert(stm, vals)
}
