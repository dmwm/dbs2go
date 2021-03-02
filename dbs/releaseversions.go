package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// ReleaseVersions DBS API
func (API) ReleaseVersions(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	releaseversions := getValues(params, "release_version")
	if len(releaseversions) > 1 {
		msg := "The releaseversions API does not support list of releaseversions"
		return 0, errors.New(msg)
	} else if len(releaseversions) == 1 {
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("releaseversions")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertReleaseVersions DBS API
func (API) InsertReleaseVersions(values Record) error {
	params := []string{"release_version"}
	if err := checkParams(values, params); err != nil {
		return err
	}
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	// get last inserted id
	pid, err := LastInsertId(tx, "RELEASE_VERSIONS", "release_version_id")
	if err != nil {
		return err
	}
	values["release_version_id"] = pid + 1
	res := InsertValuesTxt(tx, "insert_release_versions", values)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		return err
	}
	return res
}
