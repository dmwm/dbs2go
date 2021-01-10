package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// Runs DBS API
func (API) Runs(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	runs := getValues(params, "run_num")
	lfn := getValues(params, "logical_file_name")
	block := getValues(params, "block_name")
	dataset := getValues(params, "dataset")

	stm := getSQL("runs")
	if len(runs) > 1 {
		msg := "The runs API does not support list of runs"
		return 0, errors.New(msg)
	} else if len(runs) == 1 {
		conds, args = AddParam("run_num", "FL.run_num", params, conds, args)
	} else if len(lfn) == 1 {
		stm += fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID", DBOWNER)
		conds, args = AddParam("logical_file_name", "FILES.LOGICAL_FILE_NAME", params, conds, args)
	} else if len(block) == 1 {
		stm += fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.BLOCKS BLOCKS on BLOCKS.BLOCK_ID = FILES.BLOCK_ID", DBOWNER, DBOWNER)
		conds, args = AddParam("block_name", "BLOCKS.BLOCK_NAME", params, conds, args)
	} else if len(dataset) == 1 {
		stm += fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.DATASETS DATASETS on DATASETS.DATASET_ID = FILES.DATASET_ID", DBOWNER, DBOWNER)
		conds, args = AddParam("dataset", "DATASETS.DATASET", params, conds, args)
	} else {
		msg := fmt.Sprintf("No arguments for runs API")
		return 0, errors.New(msg)
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertRuns DBS API
func (API) InsertRuns(values Record) error {
	return InsertValues("insert_runs", values)
}
