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
	// TODO: implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSProcessingEra.py
	// input values: processing_version, creation_date,  create_by, description
	// businput["processing_era_id"] = self.sm.increment(conn, "SEQ_PE", tran)
	return InsertValues("insert_processing_eras", values)
}
