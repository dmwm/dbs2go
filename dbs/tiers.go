package dbs

import (
	"net/http"
)

// DataTiers DBS API
func (API) DataTiers(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("data_tier_name", "DT.DATA_TIER_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("tiers")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDataTiers DBS API
func (API) InsertDataTiers(values Record) error {
	return InsertValues("insert_tiers", values)
}
