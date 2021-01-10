package dbs

import (
	"net/http"
)

// PrimaryDSTypes DBS API
func (API) PrimaryDSTypes(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("primary_ds_type", "PDT.PRIMARY_DS_TYPE", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("primarydstypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertPrimaryDSTypes DBS API
func (API) InsertPrimaryDSTypes(values Record) error {
	return InsertValues("insert_primary_ds_types", values)
}
