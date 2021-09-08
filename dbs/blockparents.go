package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// BlockParents DBS API
func (a *API) BlockParents() error {
	var args []interface{}
	var conds []string

	// parse dataset argument
	blockparent := getValues(a.Params, "block_name")
	if len(blockparent) > 1 {
		msg := "Unsupported list of blockparent"
		return errors.New(msg)
	} else if len(blockparent) == 1 {
		conds, args = AddParam("block_name", "BC.BLOCK_NAME", a.Params, conds, args)
	}
	// get SQL statement from static area
	stm := getSQL("blockparent")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// BlockParents structure represents block parents table in DBS DB
type BlockParents struct {
	THIS_BLOCK_ID   int64 `json:"this_block_id" validate:"required,number,gt=0"`
	PARENT_BLOCK_ID int64 `json:"parent_block_id" validate:"required,number,gt=0"`
}

// Insert implementation of BlockParents
func (r *BlockParents) Insert(tx *sql.Tx) error {
	var err error
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return err
	}
	// get SQL statement from static area
	stm := getSQL("insert_fileparents")
	if utils.VERBOSE > 0 {
		log.Printf("Insert BlockParents\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.THIS_BLOCK_ID, r.PARENT_BLOCK_ID)
	return err
}

// Validate implementation of BlockParents
func (r *BlockParents) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if r.THIS_BLOCK_ID == 0 {
		return errors.New("missing this_block_id")
	}
	if r.PARENT_BLOCK_ID == 0 {
		return errors.New("missing parent_block_id")
	}
	return nil
}

// SetDefaults implements set defaults for BlockParents
func (r *BlockParents) SetDefaults() {
}

// Decode implementation for BlockParents
func (r *BlockParents) Decode(reader io.Reader) error {
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
