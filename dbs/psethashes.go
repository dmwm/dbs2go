package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// ParameterSetHashes represents Parameter Set Hashes DBS DB table
type ParameterSetHashes struct {
	PARAMETER_SET_HASH_ID int64  `json:"parameter_set_hash_id"`
	PSET_NAME             string `json:"pset_name"`
	PSET_HASH             string `json:"pset_hash" validate:"required"`
}

// Insert implementation of ParameterSetHashes
func (r *ParameterSetHashes) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PARAMETER_SET_HASH_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "PARAMETER_SET_HASHES", "parameter_set_hash_id")
			r.PARAMETER_SET_HASH_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PSH")
			r.PARAMETER_SET_HASH_ID = tid
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
	stm := getSQL("insert_psethashes")
	if utils.VERBOSE > 0 {
		log.Printf("Insert ParameterSetHashes\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PARAMETER_SET_HASH_ID, r.PSET_NAME, r.PSET_HASH)
	return err
}

// Validate implementation of ParameterSetHashes
func (r *ParameterSetHashes) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for ParameterSetHashes
func (r *ParameterSetHashes) SetDefaults() {
}

// Decode implementation for ParameterSetHashes
func (r *ParameterSetHashes) Decode(reader io.Reader) error {
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
