package dbs

import "net/http"

// AcquisitionErasCI DBS API
func (API) AcquisitionErasCi(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := ""
	// get SQL statement from static area
	stm := getSQL("acquisitioneras_ci")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertAcquisitionErasCi DBS API
func (API) InsertAcquisitionErasCi(values Record) error {
	return InsertData("insert_acquisitioneras_ci", values)
}
