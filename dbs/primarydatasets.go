package dbs

import (
	"net/http"
)

// PrimaryDatasets DBS API
func (API) PrimaryDatasets(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("primary_ds_name", "P.PRIMARY_DS_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("primarydatasets")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertPrimaryDatasets DBS API
func (API) InsertPrimaryDatasets(values Record) error {
	return InsertValues("insert_primary_datasets", values)
}
