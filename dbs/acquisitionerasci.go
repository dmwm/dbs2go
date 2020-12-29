package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// AcquisitionErasCI DBS API
func (API) AcquisitionErasCi(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	var preSession, postSession []string
	where := ""
	// parse dataset argument
	acquisitioneras := getValues(params, "acquisitionEra")
	if len(acquisitioneras) > 1 {
		msg := "The acquisitioneras API does not support list of acquisitioneras"
		return 0, errors.New(msg)
	} else if len(acquisitioneras) == 1 {
		op, val := OperatorValue(acquisitioneras[0])
		cond := fmt.Sprintf(" AE.ACQUISITION_ERA_NAME %s %s", op, placeholder("acquisition_era_name"))
		where += addCond(where, cond)
		args = append(args, val)
		preSession = append(preSession, "alter session set NLS_COMP=LINGUISTIC")
		preSession = append(preSession, "alter session set NLS_SORT=BINARY_CI")
		postSession = append(postSession, "alter session set NLS_COMP=BINARY")
		postSession = append(postSession, "alter session set NLS_SORT=BINARY")
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("acquisitioneras_ci")
	// use generic query API to fetch the results from DB
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return 0, errors.New(msg)
	}
	defer tx.Rollback()
	if err := executeSessions(tx, preSession); err != nil {
		return 0, err
	}
	r, e := executeAll(w, stm+where, args...)
	if err := executeSessions(tx, postSession); err != nil {
		return 0, err
	}
	return r, e
}

// InsertAcquisitionErasCi DBS API
func (API) InsertAcquisitionErasCi(values Record) error {
	return InsertData("insert_acquisitioneras_ci", values)
}
