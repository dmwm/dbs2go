package dbs

import "net/http"

// ParentDSTrio API
func (API) ParentDSTrio(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("datasetchildren")

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
