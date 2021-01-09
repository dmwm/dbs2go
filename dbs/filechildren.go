package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// FileChildren API
func (API) FileChildren(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	filechildren := getValues(params, "logical_file_name")
	if len(filechildren) > 1 {
		msg := "The filechildren API does not support list of filechildren"
		return 0, errors.New(msg)
	} else if len(filechildren) == 1 {
		op, val := OperatorValue(filechildren[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("filechildren")
	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileChildren DBS API
func (API) InsertFileChildren(values Record) error {
	return InsertValues("insert_file_children", values)
}
