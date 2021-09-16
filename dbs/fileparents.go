package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// FileParents API
func (a *API) FileParents() error {
	var args []interface{}
	var conds []string

	if len(a.Params) == 0 {
		msg := "logical_file_name, block_id or block_name is required for fileparents api"
		return errors.New(msg)
	}

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	blocks := getValues(a.Params, "block_name")
	if len(blocks) == 1 {
		tmpl["BlockName"] = true
		conds, args = AddParam("block_name", "B.BLOCK_NAME", a.Params, conds, args)
	}

	stm, err := LoadTemplateSQL("fileparent", tmpl)
	if err != nil {
		return err
	}

	lfns := getValues(a.Params, "logical_file_name")
	if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", a.Params, conds, args)
	} else if len(lfns) > 1 {
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
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// FileParents represents file parents DBS DB table
type FileParents struct {
	THIS_FILE_ID   int64 `json:"this_file_id" validate:"required,number,gt=0"`
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

	// check if our data already exist in DB
	var vals []interface{}
	vals = append(vals, r.THIS_FILE_ID)
	vals = append(vals, r.PARENT_FILE_ID)
	args := []string{"this_file_id", "parent_file_id"}
	if IfExistMulti(tx, "FILE_PARENTS", "this_file_id", args, vals...) {
		return nil
	}

	// get SQL statement from static area
	stm := getSQL("insert_fileparents")
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileParents\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.THIS_FILE_ID, r.PARENT_FILE_ID)
	if err != nil {
		log.Println("unable to execute", stm, "error", err)
	}
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

// FileParentRecord represents file parent DBS record
type FileParentRecord struct {
	LogicalFileName       string `json:"logical_file_name"`
	ParentLogicalFileName string `json:"parent_logical_file_name"`
}

// InsertFileParents DBS API is used by /fileparents end-point
// it accepts FileParentBlockRecord
func (a *API) InsertFileParents() error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()
	err = a.InsertFileParentsBlockTxt(tx)
	if err != nil {
		log.Println("unable to insert file parents", err)
		return err
	}
	//     if err != nil {
	//         err = a.InsertFileParentsTxt(tx)
	//         if err != nil {
	//             log.Println("unable to insert file parents", err)
	//             return err
	//         }
	//     }

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return err
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}

// InsertFileParentsTxt DBS API is used by bulkblocks API
func (a *API) InsertFileParentsTxt(tx *sql.Tx) error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/dao/Oracle/FileParent/Insert.py

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileParents record %+v", a.Params)
	}

	//     rrr := FileParentRecord
	//     lfns := getValues(a.Params, "logical_file_name")
	//     plfns := getValues(a.Params, "parent_logical_file_name")
	//     if len(lfns) != len(plfns) {
	//         msg  := "wrong number of lfns vs plfns"
	//         log.Println(msg)
	//         return errrors.New(msg)
	//     }
	//     var records []FileParentRecord
	//     for idx, lfn : range lfns {
	//         r := FileParentRecord{LogicalFileName: lfn, ParentLogicalFileName: pfn}
	//         records = append(records, r)
	//     }

	var records []FileParentRecord
	err = json.Unmarshal(data, &records)
	if err != nil {
		log.Println("fail to decode data", err, "will proceed with FileParentRecord")
		var rrr FileParentRecord
		err = json.Unmarshal(data, &rrr)
		if err != nil {
			log.Println("fail to decode data", err)
			return err
		}
		if rrr.LogicalFileName != "" {
			records = append(records, rrr)
		}
	}
	for _, rec := range records {
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
	}
	return nil
}

// FileParentBlockRecord represents file parent DBS record
type FileParentBlockRecord struct {
	BlockName         string    `json:"block_name"`
	ChildParentIDList [][]int64 `json:"child_parent_id_list"`
}

// InsertFileParentsBlockTxt DBS API
func (a *API) InsertFileParentsBlockTxt(tx *sql.Tx) error {
	// TODO: implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/dao/Oracle/FileParent/Insert2.py
	/*
	   input block_name: is a child block name.
	   input chils_parent_id_list: is a list of file id of child, parent  pair: [[cid1, pid1],[cid2,pid2],[cid3,pid3],...]
	   The requirement for this API is
	   1. All the child files belong to the block.
	   2. All the child-parent pairs are not already in DBS.
	   3. The dataset parentage is already in DBS.
	*/

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}

	var args []interface{}
	var conds []string

	var rec FileParentBlockRecord
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data as FileParentBlockRecord", err)
		return err
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileParentsBlock record %+v", rec)
	}
	//     if utils.VERBOSE > 0 {
	//         log.Printf("InsertFileParentsBlock record %+v", a.Params)
	//     }

	if len(rec.ChildParentIDList) == 0 {
		msg := "InsertFileParentsBlock API requires child_parent_id_list"
		log.Println(msg)
		return errors.New(msg)
	}

	cond := fmt.Sprintf(" B.BLOCK_NAME = %s", placeholder("block_name"))
	conds = append(conds, cond)
	args = append(args, rec.BlockName)

	//     conds, args = AddParam("block_name", "B.BLOCK_NAME", a.Params, conds, args)
	stm := getSQL("fileparents_block")
	stm = WhereClause(stm, conds)
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	// get file ids associated with given block name
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
		return errors.New(msg)
	}
	defer rows.Close()
	var bfids []int64
	for rows.Next() {
		var fid int64
		if err := rows.Scan(&fid); err != nil {
			log.Println("fail to get row.Scan, error", err)
			return err
		}
		bfids = append(bfids, fid)
	}
	// check that out file ids from block are the same as from child_parent_id_list
	var fids []int64
	for _, item := range rec.ChildParentIDList {
		fids = append(fids, item[0])
	}
	//     if vals, ok := a.Params["child_parent_id_list"]; ok {
	//         v := vals.([]int64)
	//         fids = append(fids, v[0])
	//     }
	log.Println("InsertFileParentsBlock fids", fids, "bfids", bfids)
	if len(fids) != len(bfids) {
		log.Println("block fids != file ids")
		log.Println("block ids", bfids)
		log.Println("file  ids", fids)
		msg := fmt.Sprintf("not all files present in block")
		return errors.New(msg)
	}
	// now we can loop over provided list and insert file parents
	//     if vals, ok := a.Params["child_parent_id_list"]; ok {
	//         v := vals.([]int64)
	for _, v := range rec.ChildParentIDList {
		var r FileParents
		r.THIS_FILE_ID = v[0]
		r.PARENT_FILE_ID = v[1]
		log.Println("InsertFileParentsBlock", r)
		err = r.Validate()
		if err != nil {
			log.Println("unable to validate the record", r, "error", err)
			return err
		}
		err = r.Insert(tx)
		if err != nil {
			log.Println("unable to insert FileParentsBlock record, error", err)
			return err
		}
	}
	return nil
}
