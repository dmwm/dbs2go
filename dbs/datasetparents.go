package dbs

import (
	"io"
	"net/http"
)

// DatasetParents API
func (API) DatasetParents(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("dataset", "D.DATASET", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("datasetparent")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDatasetParents DBS API
func (API) InsertDatasetParents(r io.Reader, cby string) (int64, error) {
	//     return InsertValues("insert_dataset_parents", values)
	return 0, nil
}
