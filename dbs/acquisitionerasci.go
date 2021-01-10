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
	var conds, preSession, postSession []string

	// parse dataset argument
	acquisitioneras := getValues(params, "acquisitionEra")
	if len(acquisitioneras) == 1 {
		conds, args = AddParam("acquisitionEra", "AE.ACQUISITION_ERA_NAME", params, conds, args)
		preSession = append(preSession, "alter session set NLS_COMP=LINGUISTIC")
		preSession = append(preSession, "alter session set NLS_SORT=BINARY_CI")
		postSession = append(postSession, "alter session set NLS_COMP=BINARY")
		postSession = append(postSession, "alter session set NLS_SORT=BINARY")
	}

	// get SQL statement from static area
	stm := getSQL("acquisitioneras_ci")
	stm = WhereClause(stm, conds)

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

	r, e := executeAll(w, stm, args...)
	if err := executeSessions(tx, postSession); err != nil {
		return 0, err
	}
	return r, e
}
