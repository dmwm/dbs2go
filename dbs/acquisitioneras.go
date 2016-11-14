package dbs

import (
	"fmt"
)

// acquisitioneras API
func acquisitioneras(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	acquisitioneras := getValues(params, "data_tier_name")
	if len(acquisitioneras) > 1 {
		panic("The acquisitioneras API does not support list of acquisitioneras")
	} else if len(acquisitioneras) == 1 {
		op, val := opVal(acquisitioneras[0])
		cond := fmt.Sprintf(" AE.ACQUISITION_ERA_NAME %s %s", op, placeholder("acquisition_era_name"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
