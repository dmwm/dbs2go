package dbs

import (
	"net/http"
)

// ProcessingEras DBS API
func (API) ProcessingEras(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("processing_version", "PE.PROCESSING_VERSION", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("processingeras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertProcessingEras DBS API
func (API) InsertProcessingEras(values Record) error {
	return InsertValues("insert_processing_eras", values)
}
