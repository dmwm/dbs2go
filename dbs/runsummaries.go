package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// RunSummaries DBS API
func (API) RunSummaries(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	// get SQL statement from static area
	stm := getSQL("runsummaries")

	// parse arguments
	runs := getValues(params, "run_num")
	if len(runs) > 1 {
		msg := "The runs API does not support list of runs"
		return 0, errors.New(msg)
	} else if len(runs) == 1 {
		op, val := OperatorValue(runs[0])
		cond := fmt.Sprintf(" RUN_NUM %s %s", op, placeholder("run_num"))
		conds = append(conds, cond)
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for runsummaries API")
		return 0, errors.New(msg)
	}

	dataset := getValues(params, "dataset")
	if len(dataset) == 1 {
		joins := fmt.Sprintf("JOIN %s.FILES FS ON FS.FILE_ID=FL.FILE_ID JOIN %s.DATASETS DS ON FS.DATASET_ID=DS.DATASET_ID", DBOWNER, DBOWNER)
		stm += joins
		op, val := OperatorValue(dataset[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
