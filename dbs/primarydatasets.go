package dbs

import (
	"errors"
	"fmt"
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
func (api API) InsertPrimaryDatasets(values Record) error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSPrimaryDataset.py
	// intput values: primary_ds_name, primary_ds_type, creation_date, create_by
	// insert primary_ds_type and get primary_ds_type_id
	// businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
	// insert primary_ds_name, creation_date, create_by, primary_ds_id

	params := []string{"primary_ds_name", "primary_ds_type", "creation_date", "create_by"}
	if !checkParams(values, params) {
		msg := fmt.Sprintf("Not sufficient number of parameters %s, we expect %s", values, params)
		return errors.New(msg)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	// if primary_ds_id is not given we will insert primary ds type first
	if _, ok := values["primary_ds_id"]; !ok {
		rec := make(Record)
		rec["primary_ds_type"] = values["primary_ds_type"]
		api.InsertPrimaryDSTypes(values)
		pid, err := LastInsertId(tx, "primary_ds_types", "primary_ds_id")
		if err != nil {
			return err
		}
		values["primary_ds_id"] = pid
	}
	delete(values, "primary_ds_type")
	res := InsertValues("insert_primary_datasets", values)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return res
}
