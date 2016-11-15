package dbs

import (
	"fmt"
)

// releaseversions API
func (API) ReleaseVersions(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	releaseversions := getValues(params, "release_version")
	if len(releaseversions) > 1 {
		panic("The releaseversions API does not support list of releaseversions")
	} else if len(releaseversions) == 1 {
		op, val := opVal(releaseversions[0])
		cond := fmt.Sprintf(" RV.release_version %s %s", op, placeholder("release_version"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("releaseversions")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
