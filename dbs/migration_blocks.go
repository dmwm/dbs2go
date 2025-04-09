package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"strings"

	"github.com/dmwm/dbs2go/utils"
)

// MigrationBlocks represents migration blocks table
type MigrationBlocks struct {
	MIGRATION_BLOCK_ID     int64  `json:"migration_block_id" validate:"required,number,gt=0"`
	MIGRATION_REQUEST_ID   int64  `json:"migration_request_id" validate:"required,number,gt=0"`
	MIGRATION_BLOCK_NAME   string `json:"migration_block_name" validate:"required"`
	MIGRATION_ORDER        int64  `json:"migration_order" validate:"gte=0"`
	MIGRATION_STATUS       int64  `json:"migration_status" validate:"gte=0,lte=10"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number,gt=0"`
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
			tid, err = IncrementSequence(tx, "SEQ_MB")
			r.MIGRATION_BLOCK_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "unable to increment migration block sequence number", "dbs.migration_blocks.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "fail to validate migratino block record", "dbs.migration_blocks.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_migration_blocks")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		var args []interface{}
		args = append(args, r.MIGRATION_BLOCK_ID)
		args = append(args, r.MIGRATION_REQUEST_ID)
		args = append(args, r.MIGRATION_BLOCK_NAME)
		args = append(args, r.MIGRATION_ORDER)
		args = append(args, r.MIGRATION_STATUS)
		args = append(args, r.CREATION_DATE)
		args = append(args, r.CREATE_BY)
		args = append(args, r.LAST_MODIFICATION_DATE)
		args = append(args, r.LAST_MODIFIED_BY)
		utils.PrintSQL(stm, args, "execute")
	}
	_, err = tx.Exec(stm,
		r.MIGRATION_BLOCK_ID,
		r.MIGRATION_REQUEST_ID,
		r.MIGRATION_BLOCK_NAME,
		r.MIGRATION_ORDER,
		r.MIGRATION_STATUS,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			// if we try to insert the same migration input we'll continue
			log.Printf("warning: skip %+v since it is already inserted in another request, error=%v", r, err)
			return nil
		}
		if utils.VERBOSE > 0 {
			log.Println("unable to insert migration block", err)
		}
		return Error(err, InsertMigrationBlockErrorCode, "unable to insert migration block record", "dbs.migration_blocks.Insert")
	}
	return nil
}

// Validate implementation of MigrationBlocks
func (r *MigrationBlocks) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		log.Println("validation error", err)
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
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read migration block record", "dbs.migration_blocks.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode migration block record", "dbs.migration_blocks.Decode")
	}
	return nil
}
