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
	for k, v := range values {
		if !strings.Contains(strings.ToLower(stm), k) {
			msg := fmt.Sprintf("unable to find column '%s' in %s", k, stm)
			return "", vals, errors.New(msg)
		}
		vals = append(vals, v)
		args = append(args, "?")
	}
	stm = fmt.Sprintf("%s VALUES (%s)", stm, strings.Join(args, ","))
	if utils.VERBOSE > 0 {
		log.Println("InsertValues", stm, vals, values)
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
	// get SQL statement from static area
	stm := LoadTemplateSQL(tmpl, args)
	var vals []interface{}
	for k, v := range values {
		if !strings.Contains(strings.ToLower(stm), k) {
			msg := fmt.Sprintf("unable to find column '%s' in %s", k, stm)
			return "", vals, errors.New(msg)
		}
		stm = strings.Replace(stm, fmt.Sprintf("?:%s", k), "?", -1)
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
