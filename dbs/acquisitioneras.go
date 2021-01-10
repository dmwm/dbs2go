package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// AcquisitionEras DBS API
func (API) AcquisitionEras(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	// parse dataset argument
	acquisitioneras := getValues(params, "acquisitionEra")
	if len(acquisitioneras) > 1 {
		msg := "The acquisitioneras API does not support list of acquisitioneras"
		return 0, errors.New(msg)
	} else if len(acquisitioneras) == 1 {
		op, val := OperatorValue(acquisitioneras[0])
		cond := fmt.Sprintf(" AE.ACQUISITION_ERA_NAME %s %s", op, placeholder("acquisition_era_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertAcquisitionEras DBS API
func (API) InsertAcquisitionEras(values Record) error {
	return InsertValues("insert_acquisition_eras", values)
}
