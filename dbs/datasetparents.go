package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// DatasetParents API
func (API) DatasetParents(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	datasetparent := getValues(params, "dataset")
	if len(datasetparent) > 1 {
		msg := "The datasetparent API does not support list of datasetparent"
		return 0, errors.New(msg)
	} else if len(datasetparent) == 1 {
		op, val := OperatorValue(datasetparent[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		conds = append(conds, cond)
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for datasetparent API")
		return 0, errors.New(msg)
	}

	// get SQL statement from static area
	stm := getSQL("datasetparent")
	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDatasetParents DBS API
func (API) InsertDatasetParents(values Record) error {
	return InsertValues("insert_dataset_parents", values)
}
