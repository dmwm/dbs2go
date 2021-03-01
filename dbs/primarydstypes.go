package dbs

import (
	"errors"
	"fmt"
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
	params := []string{"primary_ds_type"}
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
	pid, err := LastInsertId(tx, "primary_ds_types", "primary_ds_type_id")
	if err != nil {
		return err
	}
	values["primary_ds_type_id"] = pid + 1
	res := InsertValues("insert_primary_ds_types", values)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return res
}
