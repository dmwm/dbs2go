package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
)

// MigrationRequests represent MigrationRequests table
type MigrationRequests struct {
	MIGRATION_REQUEST_ID   int64  `json:"migration_request_id"`
	MIGRATION_URL          string `json:"migration_url"`
	MIGRATION_INPUT        string `json:"migration_input"`
	MIGRATION_STATUS       int64  `json:"migration_status"`
	CREATE_BY              string `json:"create_by"`
	CREATION_DATE          int64  `json:"creation_date"`
	LAST_MODIFIED_BY       string `json:"last_modified_by"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date"`
	RETRY_COUNT            int64  `json:"retry_count"`
}

// Insert implementation of MigrationRequests
func (r *MigrationRequests) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.MIGRATION_REQUEST_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "MIGRATION_REQUESTS", "migration_request_id")
			r.MIGRATION_REQUEST_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_MR")
			r.MIGRATION_REQUEST_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return err
	}
	// get SQL statement from static area
	stm := getSQL("insert_migration_request")
	_, err = tx.Exec(stm, r.MIGRATION_REQUEST_ID, r.MIGRATION_URL,
		r.MIGRATION_INPUT, r.MIGRATION_STATUS,
		r.CREATE_BY, r.CREATION_DATE,
		r.LAST_MODIFIED_BY, r.LAST_MODIFICATION_DATE, r.RETRY_COUNT)
	return err
}

// Validate implementation of MigrationRequests
func (r *MigrationRequests) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for MigrationRequests
func (r *MigrationRequests) SetDefaults() {
}

// Decode implementation for MigrationRequests
func (r *MigrationRequests) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	return nil
}
