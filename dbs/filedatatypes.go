package dbs

import (
	"net/http"
)

// FileDataTypes DBS API
func (API) FileDataTypes(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := ""

	// get SQL statement from static area
	stm := getSQL("file_data_types")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertFileDataTypes DBS API
func (API) InsertFileDataTypes(values Record) error {
	return InsertData("insert_file_data_types", values)
}
