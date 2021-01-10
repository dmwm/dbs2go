package dbs

import (
	"net/http"
)

// DatasetAccessTypes DBS API
func (API) DatasetAccessTypes(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("dataset_access_type", "DT.DATASET_ACCESS_TYPE", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("datasetaccesstypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDatasetAccessTypes DBS API
func (API) InsertDatasetAccessTypes(values Record) error {
	return InsertValues("insert_dataset_access_types", values)
}
