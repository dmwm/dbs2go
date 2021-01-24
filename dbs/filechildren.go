package dbs

import (
	"errors"
	"net/http"
)

// FileChildren API
func (API) FileChildren(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	if len(params) == 0 {
		return 0, errors.New("logical_file_name, block_id or block_name is required for fileparents api")
	}

	// parse dataset argument
	filechildren := getValues(params, "logical_file_name")
	if len(filechildren) > 1 {
		msg := "The filechildren API does not support list of filechildren"
		return 0, errors.New(msg)
	} else if len(filechildren) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("filechildren")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileChildren DBS API
func (API) InsertFileChildren(values Record) error {
	return InsertValues("insert_file_children", values)
}
