package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// ReleaseVersions DBS API
func (API) ReleaseVersions(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	releaseversions := getValues(params, "release_version")
	if len(releaseversions) > 1 {
		msg := "The releaseversions API does not support list of releaseversions"
		return 0, errors.New(msg)
	} else if len(releaseversions) == 1 {
		op, val := OperatorValue(releaseversions[0])
		cond := fmt.Sprintf(" RV.release_version %s %s", op, placeholder("release_version"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = "" // no arguments
	}
	// get SQL statement from static area
	stm := getSQL("releaseversions")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertReleaseVersions DBS API
func (API) InsertReleaseVersions(values Record) error {
	return InsertValues("insert_release_versions", values)
}
