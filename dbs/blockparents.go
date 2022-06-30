package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// BlockParents DBS API
func (a *API) BlockParents() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["TokenGenerator"] = ""

	// parse dataset argument
	blockparent := getValues(a.Params, "block_name")
	if len(blockparent) > 1 {
		cond := fmt.Sprintf("BC.BLOCK_NAME in %s", TokenCondition())
		// 100 is max for # of allowed blocks
		token, binds := TokenGenerator(blockparent, 100, "block_token")
		tmpl["TokenGenerator"] = token
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(blockparent) == 1 {
		conds, args = AddParam("block_name", "BC.BLOCK_NAME", a.Params, conds, args)
	}
	// get SQL statement from static area
	stm, err := LoadTemplateSQL("blockparent", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.blockparents.BlockParents")
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.blockparents.BlockParents")
	}
	return nil
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
		log.Println("unable to validate record", r, err)
		return Error(err, ValidateErrorCode, "", "dbs.blockparents.Insert")
	}
	// we first need to check if provided hlock ids exist in DB
	pbid, err := QueryRow("BLOCK_PARENTS", "PARENT_BLOCK_ID", "THIS_BLOCK_ID", r.THIS_BLOCK_ID)
	if err == nil && pbid == r.PARENT_BLOCK_ID {
		// data already in DB no need to insert anything
		return nil
	}

	// get SQL statement from static area
	stm := getSQL("insert_block_parents")
	if utils.VERBOSE > 0 {
		log.Printf("Insert BlockParents\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.THIS_BLOCK_ID, r.PARENT_BLOCK_ID)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.blockparents.Insert")
	}
	return nil
}

// Validate implementation of BlockParents
func (r *BlockParents) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if r.THIS_BLOCK_ID == 0 {
		msg := "missing this_block_id"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.blockparents.Validate")
	}
	if r.PARENT_BLOCK_ID == 0 {
		msg := "missing parent_block_id"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.blockparents.Validate")
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
		return Error(err, ReaderErrorCode, "", "dbs.blockparents.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.blockparents.Decode")
	}
	return nil
}
