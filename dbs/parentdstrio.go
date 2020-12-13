package dbs

import "net/http"

// ParentDSTrio API
func (API) ParentDSTrio(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := ""

	// get SQL statement from static area
	stm := getSQL("datasetchildren")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}
