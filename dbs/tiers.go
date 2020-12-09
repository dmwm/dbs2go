package dbs

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
)

// DataTiers API
func (API) DataTiers(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	tiers := getValues(params, "data_tier_name")
	if len(tiers) > 1 {
		msg := "The datatiers API does not support list of tiers"
		return 0, errors.New(msg)
	} else if len(tiers) == 1 {
		op, val := opVal(tiers[0])
		cond := fmt.Sprintf(" DT.DATA_TIER_NAME %s %s", op, placeholder("data_tier_name"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("tiers")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertDataTiers API
func (API) InsertDataTiers(values Record) error {
	// get SQL statement from static area
	stm := getSQL("insert_tiers")
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
	log.Println("InsertDataTiers", stm, vals, values)
	// use generic query API to fetch the results from DB
	return insert(stm, vals)
}
