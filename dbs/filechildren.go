package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// FileChildren API
func (API) FileChildren(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	filechildren := getValues(params, "logical_file_name")
	if len(filechildren) > 1 {
		msg := "The filechildren API does not support list of filechildren"
		return 0, errors.New(msg)
	} else if len(filechildren) == 1 {
		op, val := OperatorValue(filechildren[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("filechildren")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertFileChildren DBS API
func (API) InsertFileChildren(values Record) error {
	return InsertData("insert_file_children", values)
}
