package dbs

import (
	"fmt"
)

// primarydstypes API
func (API) Primarydstypes(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	primarydstypes := getValues(params, "primary_ds_type")
	if len(primarydstypes) > 1 {
		panic("The primarydstypes API does not support list of primarydstypes")
	} else if len(primarydstypes) == 1 {
		op, val := opVal(primarydstypes[0])
		cond := fmt.Sprintf(" PDT.PRIMARY_DS_TYPE %s %s", op, placeholder("primary_ds_type"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("primarydstypes")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
