package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
)

// PrimaryDSTypes DBS API
func (a *API) PrimaryDSTypes() error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("primary_ds_type", "PDT.PRIMARY_DS_TYPE", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("primarydstypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.primarydstypes.PrimaryDSTypes")
	}
	return nil
}

// PrimaryDSTypes represents primary ds types DBS DB table
type PrimaryDSTypes struct {
	PRIMARY_DS_TYPE_ID int64  `json:"primary_ds_type_id"`
	PRIMARY_DS_TYPE    string `json:"primary_ds_type" validate:"required"`
}

// Insert implementation of PrimaryDSTypes
func (r *PrimaryDSTypes) Insert(tx *sql.Tx) error {
	var err error
	if r.PRIMARY_DS_TYPE_ID == 0 {
		// there is no SEQ_XXX for this table, will use LastInsertId
		pid, err := LastInsertID(tx, "PRIMARY_DS_TYPES", "primary_ds_type_id")
		if err != nil {
			return Error(err, LastInsertErrorCode, "", "dbs.primarydstypes.Insert")
		}
		r.PRIMARY_DS_TYPE_ID = pid + 1
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.primarydstypes.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_primary_ds_types")
	_, err = tx.Exec(stm, r.PRIMARY_DS_TYPE_ID, r.PRIMARY_DS_TYPE)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.primarydstypes.Insert")
	}
	return nil
}

// Validate implementation of PrimaryDSTypes
func (r *PrimaryDSTypes) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for PrimaryDSTypes
func (r *PrimaryDSTypes) SetDefaults() {
}

// Decode implementation for PrimaryDSTypes
func (r *PrimaryDSTypes) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.primarydstypes.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.primarydstypes.Decode")
	}
	return nil
}

// InsertPrimaryDSTypes DBS API
func (a *API) InsertPrimaryDSTypes() error {
	err := insertRecord(&PrimaryDSTypes{}, a.Reader)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.primarydstypes.InsertPrimaryDSTypes")
	}
	return nil
}
