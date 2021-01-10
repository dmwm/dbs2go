package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// DatasetAccessTypes DBS API
func (API) DatasetAccessTypes(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	datasetaccesstypes := getValues(params, "dataset_access_type")
	if len(datasetaccesstypes) > 1 {
		msg := "The datasetaccesstypes API does not support list of datasetaccesstypes"
		return 0, errors.New(msg)
	} else if len(datasetaccesstypes) == 1 {
		op, val := OperatorValue(datasetaccesstypes[0])
		cond := fmt.Sprintf(" DT.DATASET_ACCESS_TYPE %s %s", op, placeholder("dataset_access_type"))
		conds = append(conds, cond)
		args = append(args, val)
	}
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
