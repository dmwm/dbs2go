package dbs

import (
	"log"
	"net/http"
)

// AcquisitionEras DBS API
func (API) AcquisitionEras(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	conds, args = AddParam("acquisitionEra", "AE.ACQUISITION_ERA_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertAcquisitionEras DBS API
func (API) InsertAcquisitionEras(params Record) error {
	// TODO: implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSAcquisitionEra.py
	// input values: acquisition_era_name, creation_date, start_date, end_date, create_by
	// businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)
	log.Println("InsertAE", params)
	return InsertValues("insert_acquisition_eras", params)
}

// UpdateAcquisitionEras DBS API
func (API) UpdateAcquisitionEras(params Record) error {
	// TODO: implement the following logic
	// input values: acquisition_era_name ="", end_date=0
	// businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)
	log.Println("InsertAE", params)
	return InsertValues("insert_acquisition_eras", params)
}
