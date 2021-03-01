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

	conds, args = AddParam("acquisitionEra", "AE.ACQUISITION_ERA_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertAcquisitionEras DBS API
func (API) InsertAcquisitionEras(values Record) error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSAcquisitionEra.py
	// input values: acquisition_era_name, creation_date, start_date, end_date, create_by
	// businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)
	params := []string{"acquisition_era_name", "creation_date", "start_date", "end_date", "create_by"}
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

	if _, ok := values["acquisition_era_id"]; !ok {
		sid, err := IncrementSequence(tx, "SEQ_AQE")
		if err != nil {
			tx.Rollback()
			return err
		}
		values["acquisition_era_id"] = sid + 1
	}
	res := InsertValues("insert_acquisition_eras", values)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return res
}

// UpdateAcquisitionEras DBS API
func (API) UpdateAcquisitionEras(values Record) error {
	// TODO: implement the following logic
	// input values: acquisition_era_name ="", end_date=0
	// businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)
	return InsertValues("insert_acquisition_eras", values)
}
