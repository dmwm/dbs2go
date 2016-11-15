package dbs

import (
	"fmt"
)

// runs API
func (API) Runs(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := ""

	// parse dataset argument
	runs := getValues(params, "run_num")
	lfn := getValues(params, "logical_file_name")
	block := getValues(params, "block_name")
	dataset := getValues(params, "dataset")
	if len(runs) > 1 {
		panic("The runs API does not support list of runs")
	} else if len(runs) == 1 {
		op, val := opVal(runs[0])
		cond := fmt.Sprintf(" FL.run_num %s %s", op, placeholder("run_num"))
		where += addCond(where, cond)
		args = append(args, val)
	} else if len(lfn) == 1 {
		_, val := opVal(lfn[0])
		cond := fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID WHERE FILES.LOGICAL_FILE_NAME = %s", DBOWNER, placeholder("logical_file_name"))
		where += cond
		args = append(args, val)
	} else if len(block) == 1 {
		_, val := opVal(block[0])
		cond := fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.BLOCKS BLOCKS on BLOCKS.BLOCK_ID = FILES.BLOCK_ID WHERE BLOCKS.BLOCK_NAME = %s", DBOWNER, DBOWNER, placeholder("block_name"))
		where += cond
		args = append(args, val)
	} else if len(dataset) == 1 {
		_, val := opVal(dataset[0])
		cond := fmt.Sprintf(" inner join %s.FILES FILES on FILES.FILE_ID = FL.FILE_ID inner join %s.DATASETS DATASETS on DATASETS.DATASET_ID = FILES.DATASET_ID WHERE DATASETS.DATASET = %s", DBOWNER, DBOWNER, placeholder("dataset"))
		where += cond
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for runs API")
		panic(msg)
	}
	// get SQL statement from static area
	stm := getSQL("runs")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
