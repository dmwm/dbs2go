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
	// TODO: implement the following
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSDataTier.py
	// input values: data_tier_name, creation_date, create_by
	// businput["data_tier_id"] = self.sm.increment(conn, "SEQ_DT" )
	// businput["data_tier_name"] = businput["data_tier_name"].upper()

	return InsertValues("insert_tiers", values)
}
