package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// Files DBS API
func (API) Files(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	files := getValues(params, "logical_file_name")
	if len(files) > 1 {
		msg := "The files API does not support list of files"
		return 0, errors.New(msg)
	} else if len(files) == 1 {
		op, val := opVal(files[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		msg := "The files API does not support list of datasets"
		return 0, errors.New(msg)
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	block_names := getValues(params, "block_name")
	if len(block_names) > 1 {
		msg := "The files API does not support list of block_names"
		return 0, errors.New(msg)
	} else if len(block_names) == 1 {
		op, val := opVal(block_names[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("files")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertFiles DBS API
func (API) InsertFiles(values Record) error {
	return InsertData("insert_files", values)
}
