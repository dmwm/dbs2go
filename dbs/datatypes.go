package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// DataTypes DBS API
func (API) DataTypes(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	datatypes := getValues(params, "datatype")
	if len(datatypes) > 1 {
		msg := "The datatypes API does not support list of datatypes"
		return 0, errors.New(msg)
	} else if len(datatypes) == 1 {
		op, val := OperatorValue(datatypes[0])
		cond := fmt.Sprintf(" DT.datatype %s %s", op, placeholder("datatype"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("datatypes")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertDataTypes DBS API
func (API) InsertDataTypes(values Record) error {
	return InsertValues("insert_data_types", values)
}
