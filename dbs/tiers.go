package dbs

import (
	"fmt"
)

// tiers API
func tiers(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	tiers := getValues(params, "data_tier_name")
	if len(tiers) > 1 {
		panic("The tiers API does not support list of tiers")
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
	return executeAll(stm+where, args...)
}
