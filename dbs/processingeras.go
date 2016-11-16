package dbs

import (
	"fmt"
)

// processingeras API
func (API) ProcessingEras(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	processingeras := getValues(params, "processing_version")
	if len(processingeras) > 1 {
		msg := "The processingeras API does not support list of processingeras"
		return errorRecord(msg)
	} else if len(processingeras) == 1 {
		op, val := opVal(processingeras[0])
		cond := fmt.Sprintf(" PE.PROCESSING_VERSION %s %s", op, placeholder("processing_version"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("processingeras")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
