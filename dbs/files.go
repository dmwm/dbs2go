package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// Files DBS API
func (API) Files(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	files := getValues(params, "logical_file_name")
	if len(files) > 1 {
		msg := "The files API does not support list of files"
		return 0, errors.New(msg)
	} else if len(files) == 1 {
		op, val := OperatorValue(files[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		msg := "The files API does not support list of datasets"
		return 0, errors.New(msg)
	} else if len(datasets) == 1 {
		op, val := OperatorValue(datasets[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	block_names := getValues(params, "block_name")
	if len(block_names) > 1 {
		msg := "The files API does not support list of block_names"
		return 0, errors.New(msg)
	} else if len(block_names) == 1 {
		op, val := OperatorValue(block_names[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("files")
	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFiles DBS API
func (API) InsertFiles(values Record) error {
	return InsertValues("insert_files", values)
}
