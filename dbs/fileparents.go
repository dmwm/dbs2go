package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// FileParents API
func (API) FileParents(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	fileparent := getValues(params, "logical_file_name")
	if len(fileparent) > 1 {
		msg := "The fileparent API does not support list of fileparent"
		return 0, errors.New(msg)
	} else if len(fileparent) == 1 {
		op, val := OperatorValue(fileparent[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("fileparent")
	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileParents DBS API
func (API) InsertFileParents(values Record) error {
	return InsertValues("insert_file_parents", values)
}
