package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
)

// MigrationRequest represent MigrationRequest table
type MigrationRequest struct {
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

// Insert implementation of MigrationRequest
func (r *MigrationRequest) Insert(tx *sql.Tx) error {
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

// Validate implementation of MigrationRequest
func (r *MigrationRequest) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for MigrationRequest
func (r *MigrationRequest) SetDefaults() {
}

// Decode implementation for MigrationRequest
func (r *MigrationRequest) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
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

// MigrationRequests fetches migration requests from migration table
func MigrationRequests(mid int64) ([]MigrationRequest, error) {
	var records []MigrationRequest

	// query MigrationRequest table and fetch all non-completed requests
	var args []interface{}
	var conds []string
	stm := getSQL("migration_requests")
	if mid > 0 {
		cond := fmt.Sprintf(" MR.MIGRATION_REQUEST_ID = %s", placeholder("migration_request_id"))
		conds = append(conds, cond)
		args = append(args, mid)
		stm = WhereClause(stm, conds)
	}

	if MigrationDB == nil {
		return records, errors.New("MigrationDB access is closed")
	}

	// execute sql statement
	tx, err := MigrationDB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return records, errors.New(msg)
	}
	defer tx.Rollback()
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
		return records, errors.New(msg)
	}
	defer rows.Close()
	for rows.Next() {
		var mid, migRetryCount, migCreationDate, migLastModificationDate, migStatus int64
		var migURL, migInput, migCreateBy, migLastModifiedBy string
		err := rows.Scan(
			&mid,
			&migURL,
			&migInput,
			&migStatus,
			&migCreateBy,
			&migCreationDate,
			&migLastModifiedBy,
			&migLastModificationDate,
			&migRetryCount,
		)
		if err != nil {
			msg := fmt.Sprintf("unable to scan DB results %s", err)
			return records, errors.New(msg)
		}
		rec := MigrationRequest{
			MIGRATION_REQUEST_ID:   mid,
			MIGRATION_URL:          migURL,
			MIGRATION_INPUT:        migInput,
			MIGRATION_STATUS:       migStatus,
			CREATE_BY:              migCreateBy,
			CREATION_DATE:          migCreationDate,
			LAST_MODIFIED_BY:       migLastModifiedBy,
			LAST_MODIFICATION_DATE: migLastModificationDate,
			RETRY_COUNT:            migRetryCount,
		}
		records = append(records, rec)
	}
	if err = rows.Err(); err != nil {
		msg := fmt.Sprintf("rows error %v", err)
		return records, errors.New(msg)
	}
	return records, nil
}
