package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// ApplicationExecutables structure describe associative table in DBS DB
type ApplicationExecutables struct {
	APP_EXEC_ID int64  `json:"app_exec_id"`
	APP_NAME    string `json:"app_name" validate:"required"`
}

// Insert implementation of ApplicationExecutables
func (r *ApplicationExecutables) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.APP_EXEC_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "APPLICATION_EXECUTABLES", "app_exec_id")
			r.APP_EXEC_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_AE")
			r.APP_EXEC_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "", "dbs.appexec.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.appexec.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_appexec")
	if utils.VERBOSE > 0 {
		log.Printf("Insert ApplicationExecutables\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.APP_EXEC_ID, r.APP_NAME)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert ApplicationExecutables record, error", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.appexec.Insert")
	}
	return nil
}

// Validate implementation of ApplicationExecutables
func (r *ApplicationExecutables) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for ApplicationExecutables
func (r *ApplicationExecutables) SetDefaults() {
}

// Decode implementation for ApplicationExecutables
func (r *ApplicationExecutables) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.appexec.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.appexec.Decode")
	}
	return nil
}

// InsertApplicationExecutables DBS API
func (a *API) InsertApplicationExecutables() error {
	err := insertRecord(&ApplicationExecutables{}, a.Reader)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.appexec.InsertApplicationExecutables")
	}
	return nil
}
