package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// PrimaryDatasets DBS API
func (API) PrimaryDatasets(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	primarydatasets := getValues(params, "primary_ds_name")
	if len(primarydatasets) > 1 {
		msg := "The primarydatasets API does not support list of primarydatasets"
		return 0, errors.New(msg)
	} else if len(primarydatasets) == 1 {
		op, val := OperatorValue(primarydatasets[0])
		cond := fmt.Sprintf(" P.PRIMARY_DS_NAME %s %s", op, placeholder("primary_ds_name"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("primarydatasets")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertPrimaryDatasets DBS API
func (API) InsertPrimaryDatasets(values Record) error {
	return InsertValues("insert_primary_datasets", values)
}
