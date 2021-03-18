package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// BranchHashes
type BranchHashes struct {
	BRANCH_HASH_ID int64  `json:"branch_hash_id"`
	BRANCH_HASH    string `json:"branch_hash"`
	CONTENT        string `json:"content"`
}

// Insert implementation of BranchHashes
func (r *BranchHashes) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.BRANCH_HASH_ID == 0 {
		// there is no SEQ_BH, will use LastInsertId
		tid, err = LastInsertID(tx, "BRANCH_HASHES", "branch_hash_id")
		r.BRANCH_HASH_ID = tid + 1
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
	stm := getSQL("insert_branch_hashes")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_branch_hashes_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert BranchHashes\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.BRANCH_HASH_ID, r.BRANCH_HASH, r.CONTENT)
	return err
}

// Validate implementation of BranchHashes
func (r *BranchHashes) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for BranchHashes
func (r *BranchHashes) SetDefaults() {
	// TODO: clarify what is requried and what should be defaults
	if r.BRANCH_HASH == "" {
		r.BRANCH_HASH = "branch-hash"
	}
	if r.CONTENT == "" {
		r.CONTENT = "branch-hash content"
	}
}

// Decode implementation for BranchHashes
func (r *BranchHashes) Decode(reader io.Reader) error {
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

// InsertBranchHashes DBS API
func (API) InsertBranchHashes(r io.Reader, cby string) error {
	return insertRecord(&BranchHashes{}, r)
}
