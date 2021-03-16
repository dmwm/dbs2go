package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// ApplicationExecutables
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
			tid, err = LastInsertId(tx, "APPLICATION_EXECUTABLES", "app_exec_id")
			r.APP_EXEC_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_AE")
			r.APP_EXEC_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_appexec")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_appexec_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert ApplicationExecutables\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.APP_EXEC_ID, r.APP_NAME)
	return err
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
func (r *ApplicationExecutables) Decode(reader io.Reader) (int64, error) {
	// init record with given data record
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return 0, err
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return 0, err
	}
	size := int64(len(data))
	return size, nil
}

// InsertApplicationExecutables DBS API
func (API) InsertApplicationExecutables(r io.Reader, cby string) (int64, error) {
	return insertRecord(&ApplicationExecutables{}, r)
}
