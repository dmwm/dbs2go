package dbs

import (
	"fmt"
)

// files API
func (API) Files(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	files := getValues(params, "logical_file_name")
	if len(files) > 1 {
		panic("The files API does not support list of files")
	} else if len(files) == 1 {
		op, val := opVal(files[0])
		cond := fmt.Sprintf(" B.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		panic("The files API does not support list of datasets")
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("files")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
