package dbs

import (
	"net/http"
)

// ProcessedDatasets DBS API
func (API) ProcessedDatasets(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := ""

	// get SQL statement from static area
	stm := getSQL("processed_datasets")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertProcessedDatasets DBS API
func (API) InsertProcessedDatasets(values Record) error {
	return InsertValues("insert_processed_datasets", values)
}
