package dbs

import (
	"fmt"
)

// datatypes API
func (API) DataTypes(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	datatypes := getValues(params, "datatype")
	if len(datatypes) > 1 {
		msg := "The datatypes API does not support list of datatypes"
		return errorRecord(msg)
	} else if len(datatypes) == 1 {
		op, val := opVal(datatypes[0])
		cond := fmt.Sprintf(" DT.datatype %s %s", op, placeholder("datatype"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("datatypes")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
