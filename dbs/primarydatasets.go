package dbs

import (
	"net/http"
)

// PrimaryDatasets DBS API
func (API) PrimaryDatasets(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("primary_ds_name", "P.PRIMARY_DS_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("primarydatasets")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertPrimaryDatasets DBS API
func (API) InsertPrimaryDatasets(values Record) error {
	// TODO: implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSPrimaryDataset.py
	// intput values: primary_ds_name, primary_ds_type, creation_date, create_by
	// insert primary_ds_type and get primary_ds_type_id
	// businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
	// insert primary_ds_name, creation_date, create_by, primary_ds_id
	return InsertValues("insert_primary_datasets", values)
}
