package dbs

import (
	"errors"
	"io"
	"net/http"
)

// DatasetChildren API
func (API) DatasetChildren(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	datasetchildren := getValues(params, "dataset")
	if len(datasetchildren) > 1 {
		msg := "The datasetchildren API does not support list of datasetchildren"
		return 0, errors.New(msg)
	} else if len(datasetchildren) == 1 {
		conds, args = AddParam("dataset", "D.DATASET", params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("datasetchildren")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDatasetChildren DBS API
func (API) InsertDatasetChildren(r io.Reader, cby string) error {
	//     return InsertValues("insert_dataset_children", values)
	return nil
}
