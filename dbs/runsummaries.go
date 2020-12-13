package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// RunSummaries DBS API
func (API) RunSummaries(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := " WHERE "
	// get SQL statement from static area
	stm := getSQL("runsummaries")

	// parse arguments
	runs := getValues(params, "run_num")
	if len(runs) > 1 {
		msg := "The runs API does not support list of runs"
		return 0, errors.New(msg)
	} else if len(runs) == 1 {
		_, val := opVal(runs[0])
		cond := fmt.Sprintf(" RUN_NUM = %s", placeholder("run_num"))
		args = append(args, val)
		where += addCond(where, cond)
	} else {
		msg := fmt.Sprintf("No arguments for runsummaries API")
		return 0, errors.New(msg)
	}
	dataset := getValues(params, "dataset")
	if len(dataset) == 1 {
		joins := fmt.Sprintf("JOIN %s.FILES FS ON FS.FILE_ID=FL.FILE_ID JOIN %s.DATASETS DS ON FS.DATASET_ID=DS.DATASET_ID", DBOWNER, DBOWNER)
		stm += joins
		_, val := opVal(dataset[0])
		args = append(args, val)
		cond := fmt.Sprintf(" DS.DATASET = %s", placeholder("dataset"))
		where += addCond(where, cond)
	}
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}
