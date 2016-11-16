package dbs

import (
	"fmt"
)

// primarydatasets API
func (API) PrimaryDatasets(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	primarydatasets := getValues(params, "primary_ds_name")
	if len(primarydatasets) > 1 {
		msg := "The primarydatasets API does not support list of primarydatasets"
		return errorRecord(msg)
	} else if len(primarydatasets) == 1 {
		op, val := opVal(primarydatasets[0])
		cond := fmt.Sprintf(" P.PRIMARY_DS_NAME %s %s", op, placeholder("primary_ds_name"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("primarydatasets")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
