package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
)

// Migration_Blocks represents migratino blocks table
type MigrationBlocks struct {
	MIGRATION_BLOCK_ID     int64  `json:"migration_block_id"`
	MIGRATION_REQUEST_ID   int64  `json:"migration_request_id"`
	MIGRATION_BLOCK_NAME   string `json:"migration_block_name"`
	MIGRATION_ORDER        int64  `json:"migration_order"`
	MIGRATION_STATUS       int64  `json:"migration_status"`
	CREATE_BY              string `json:"create_by"`
	CREATION_DATE          int64  `json:"creation_date"`
	LAST_MODIFIED_BY       string `json:"last_modified_by"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date"`
}

// Insert implementation of MigrationBlocks
func (r *MigrationBlocks) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.MIGRATION_BLOCK_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "MIGRATION_BLOCKS", "migration_block_id")
			r.MIGRATION_BLOCK_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_MR")
			r.MIGRATION_BLOCK_ID = tid
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
	stm := getSQL("insert_migration_blocks")
	_, err = tx.Exec(stm, r.MIGRATION_BLOCK_ID, r.MIGRATION_REQUEST_ID,
		r.MIGRATION_BLOCK_NAME, r.MIGRATION_ORDER,
		r.CREATE_BY, r.CREATION_DATE,
		r.LAST_MODIFIED_BY, r.LAST_MODIFICATION_DATE)
	return err
}

// Validate implementation of MigrationBlocks
func (r *MigrationBlocks) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for MigrationBlocks
func (r *MigrationBlocks) SetDefaults() {
}

// Decode implementation for MigrationBlocks
func (r *MigrationBlocks) Decode(reader io.Reader) error {
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
