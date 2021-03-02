package dbs

import (
	"database/sql"
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

// InsertValuesTxt API
func InsertValuesTxt(tx *sql.Tx, tmpl string, values Record) error {
	stm, vals, err := StatementValues(tmpl, values)
	if err != nil {
		return err
	}
	_, err = tx.Exec(stm, vals...)
	return err
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

// InsertTemplateValuesTxt API
func InsertTemplateValuesTxt(tx *sql.Tx, tmpl string, args, values Record) error {
	stm, vals, err := StatementTemplateValues(tmpl, args, values)
	if err != nil {
		return err
	}
	_, err = tx.Exec(stm, vals...)
	return err
}

// InsertPlainValuesTxt API
func InsertPlainValuesTxt(tx *sql.Tx, tmpl string, values Record) error {
	var vals []interface{}
	stm := getSQL(tmpl)
	if utils.VERBOSE > 0 {
		log.Printf("initial statement\n### %s\n", stm)
	}
	var keys []string
	for {
		for k, v := range values {
			if !strings.Contains(strings.ToLower(stm), k) {
				msg := fmt.Sprintf("unable to find column '%s' in %s", k, stm)
				return errors.New(msg)
			}
			// replace all bind names with ?
			pat := fmt.Sprintf(":%s:", k)
			log.Println("replace", k, v)
			count := strings.Count(stm, pat)
			if count > 0 {
				stm = strings.Replace(stm, pat, "?", 1)
				vals = append(vals, v)
				keys = append(keys, k)
				log.Println("stm", stm, "\n", keys, "\n", vals)
			} else {
				log.Println("skip", k)
			}
		}
		if !strings.Contains(stm, ":") {
			break
		}
	}
	if utils.VERBOSE > 0 {
		log.Printf("final statement\n### %s\n%v\n%v", stm, keys, vals)
	}
	_, err := tx.Exec(stm, vals...)
	if err != nil {
		log.Printf("DB error\n### %s\n%v\n%v", stm, vals, err)
	}
	return err
}
