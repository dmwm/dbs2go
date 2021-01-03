package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// DataTiers DBS API
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
		op, val := OperatorValue(tiers[0])
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

// InsertDataTiers DBS API
func (API) InsertDataTiers(values Record) error {
	return InsertValues("insert_tiers", values)
}
