package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// acquisitioneras API
func (API) AcquisitionEras(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	acquisitioneras := getValues(params, "data_tier_name")
	if len(acquisitioneras) > 1 {
		msg := "The acquisitioneras API does not support list of acquisitioneras"
		return 0, errors.New(msg)
	} else if len(acquisitioneras) == 1 {
		op, val := opVal(acquisitioneras[0])
		cond := fmt.Sprintf(" AE.ACQUISITION_ERA_NAME %s %s", op, placeholder("acquisition_era_name"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}
