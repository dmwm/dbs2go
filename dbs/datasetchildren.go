package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// DatasetChildren API
func (API) DatasetChildren(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	datasetchildren := getValues(params, "dataset")
	if len(datasetchildren) > 1 {
		msg := "The datasetchildren API does not support list of datasetchildren"
		return 0, errors.New(msg)
	} else if len(datasetchildren) == 1 {
		op, val := OperatorValue(datasetchildren[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for datasetchildren API")
		return 0, errors.New(msg)
	}
	// get SQL statement from static area
	stm := getSQL("datasetchildren")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertDatasetChildren DBS API
func (API) InsertDatasetChildren(values Record) error {
	return InsertValues("insert_dataset_children", values)
}
