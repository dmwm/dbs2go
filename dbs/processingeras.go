package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// ProcessingEras DBS API
func (API) ProcessingEras(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	processingeras := getValues(params, "processing_version")
	if len(processingeras) > 1 {
		msg := "The processingeras API does not support list of processingeras"
		return 0, errors.New(msg)
	} else if len(processingeras) == 1 {
		op, val := OperatorValue(processingeras[0])
		cond := fmt.Sprintf(" PE.PROCESSING_VERSION %s %s", op, placeholder("processing_version"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("processingeras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertProcessingEras DBS API
func (API) InsertProcessingEras(values Record) error {
	return InsertValues("insert_processing_eras", values)
}
