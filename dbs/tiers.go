package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// DataTiers DBS API
func (API) DataTiers(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	tiers := getValues(params, "data_tier_name")
	if len(tiers) > 1 {
		msg := "The datatiers API does not support list of tiers"
		return 0, errors.New(msg)
	} else if len(tiers) == 1 {
		op, val := OperatorValue(tiers[0])
		cond := fmt.Sprintf(" DT.DATA_TIER_NAME %s %s", op, placeholder("data_tier_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("tiers")
	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDataTiers DBS API
func (API) InsertDataTiers(values Record) error {
	return InsertValues("insert_tiers", values)
}
