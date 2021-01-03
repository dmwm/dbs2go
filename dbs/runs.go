package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// Runs DBS API
func (API) Runs(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := ""

	// parse dataset argument
	runs := getValues(params, "run_num")
	lfn := getValues(params, "logical_file_name")
	block := getValues(params, "block_name")
	dataset := getValues(params, "dataset")
	if len(runs) > 1 {
		msg := "The runs API does not support list of runs"
		return 0, errors.New(msg)
	} else if len(runs) == 1 {
		op, val := OperatorValue(runs[0])
		cond := fmt.Sprintf(" FL.run_num %s %s", op, placeholder("run_num"))
		where += addCond(where, cond)
		args = append(args, val)
	} else if len(lfn) == 1 {
		_, val := OperatorValue(lfn[0])
		cond := fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID WHERE FILES.LOGICAL_FILE_NAME = %s", DBOWNER, placeholder("logical_file_name"))
		where += cond
		args = append(args, val)
	} else if len(block) == 1 {
		_, val := OperatorValue(block[0])
		cond := fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.BLOCKS BLOCKS on BLOCKS.BLOCK_ID = FILES.BLOCK_ID WHERE BLOCKS.BLOCK_NAME = %s", DBOWNER, DBOWNER, placeholder("block_name"))
		where += cond
		args = append(args, val)
	} else if len(dataset) == 1 {
		_, val := OperatorValue(dataset[0])
		cond := fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.DATASETS DATASETS on DATASETS.DATASET_ID = FILES.DATASET_ID WHERE DATASETS.DATASET = %s", DBOWNER, DBOWNER, placeholder("dataset"))
		where += cond
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for runs API")
		return 0, errors.New(msg)
	}
	// get SQL statement from static area
	stm := getSQL("runs")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertRuns DBS API
func (API) InsertRuns(values Record) error {
	return InsertValues("insert_runs", values)
}
