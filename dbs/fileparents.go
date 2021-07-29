package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// FileParents API
func (API) FileParents(params Record, w http.ResponseWriter) error {
	var args []interface{}
	var conds []string

	if len(params) == 0 {
		msg := "logical_file_name, block_id or block_name is required for fileparents api"
		return errors.New(msg)
	}

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	blocks := getValues(params, "block_name")
	if len(blocks) == 1 {
		tmpl["BlockName"] = true
		conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	}

	stm, err := LoadTemplateSQL("fileparent", tmpl)
	if err != nil {
		return err
	}

	lfns := getValues(params, "logical_file_name")
	if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
	} else {
		token, binds := TokenGenerator(lfns, 30, "lfn_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME in %s", TokenCondition())
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// FileParents
type FileParents struct {
	THIS_FILE_ID   int64 `json:"this_file_id"`
	PARENT_FILE_ID int64 `json:"parent_file_id" validate:"required,number,gt=0"`
}

// Insert implementation of FileParents
func (r *FileParents) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.THIS_FILE_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "FILE_PARENTS", "this_file_id")
			r.THIS_FILE_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_FP")
			r.THIS_FILE_ID = tid
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
	stm := getSQL("insert_fileparents")
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileParents\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.THIS_FILE_ID, r.PARENT_FILE_ID)
	return err
}

// Validate implementation of FileParents
func (r *FileParents) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if r.THIS_FILE_ID == 0 {
		return errors.New("missing this_file_id")
	}
	if r.PARENT_FILE_ID == 0 {
		return errors.New("missing parent_file_id")
	}
	return nil
}

// SetDefaults implements set defaults for FileParents
func (r *FileParents) SetDefaults() {
}

// Decode implementation for FileParents
func (r *FileParents) Decode(reader io.Reader) error {
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

type FileParentRecord struct {
	LogicalFileName       string `json:"logical_file_name"`
	ParentLogicalFileName string `json:"parent_logical_file_name"`
}

// InsertFileParents DBS API
func (api API) InsertFileParents(r io.Reader, cby string) error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()
	err = api.InsertFileParentsTxt(tx, r, cby)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return err
	}
	return err
}

// InsertFileParents DBS API
func (API) InsertFileParentsTxt(tx *sql.Tx, r io.Reader, cby string) error {
	// TODO: implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSFile.py
	/*
	   input block_name: is a child block name.
	   input chils_parent_id_list: is a list of file id of child, parent  pair: [[cid1, pid1],[cid2,pid2],[cid3,pid3],...]
	   The requirment for this API is
	   1. All the child files belong to the block.
	   2. All the child-parent pairs are not already in DBS.
	   3. The dataset parentage is already in DBS.
	*/
	// read given input
	data, err := ioutil.ReadAll(r)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	var rec FileParentRecord
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileParents record %+v", rec)
	}
	// get file id for given lfn
	fid, err := GetID(tx, "FILES", "file_id", "logical_file_name", rec.LogicalFileName)
	if err != nil {
		log.Println("unable to find file_id for", rec.LogicalFileName)
		return err
	}
	pid, err := GetID(tx, "FILES", "file_id", "logical_file_name", rec.ParentLogicalFileName)
	if err != nil {
		log.Println("unable to find file_id for", rec.ParentLogicalFileName)
		return err
	}
	var rrr FileParents
	rrr.THIS_FILE_ID = fid
	rrr.PARENT_FILE_ID = pid
	err = rrr.Validate()
	if err != nil {
		return err
	}
	err = rrr.Insert(tx)
	if err != nil {
		return err
	}
	return nil
}
