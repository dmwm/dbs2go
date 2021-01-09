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
		op, val := OperatorValue(runs[0])
		cond := fmt.Sprintf(" FL.run_num %s %s", op, placeholder("run_num"))
		conds = append(conds, cond)
		args = append(args, val)
	} else if len(lfn) == 1 {
		op, val := OperatorValue(lfn[0])
		stm += fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID", DBOWNER)
		cond := fmt.Sprintf("FILES.LOGICAL_FILE_NAME = %s", op, placeholder("logical_file_name"))
		conds = append(conds, cond)
		args = append(args, val)
	} else if len(block) == 1 {
		op, val := OperatorValue(block[0])
		stm += fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.BLOCKS BLOCKS on BLOCKS.BLOCK_ID = FILES.BLOCK_ID", DBOWNER, DBOWNER)
		cond := fmt.Sprintf("BLOCKS.BLOCK_NAME %s %s", op, placeholder("block_name"))
		conds = append(conds, cond)
		args = append(args, val)
	} else if len(dataset) == 1 {
		op, val := OperatorValue(dataset[0])
		stm += fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.DATASETS DATASETS on DATASETS.DATASET_ID = FILES.DATASET_ID", DBOWNER, DBOWNER)
		cond := fmt.Sprintf("DATASETS.DATASET %s %s", op, placeholder("dataset"))
		conds = append(conds, cond)
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for runs API")
		return 0, errors.New(msg)
	}

	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertRuns DBS API
func (API) InsertRuns(values Record) error {
	return InsertValues("insert_runs", values)
}
