package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// FileParents API
func (a *API) FileParents() error {
	var args []interface{}
	var conds []string

	if len(a.Params) == 0 {
		msg := "logical_file_name, block_id or block_name is required for fileparents api"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.fileparents.FielParents")
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
		return Error(err, LoadErrorCode, "", "dbs.fileparents.FielParents")
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
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.fileparents.FielParents")
	}
	return nil
}

// FileParents represents file parents DBS DB table
type FileParents struct {
	THIS_FILE_ID   int64 `json:"this_file_id" validate:"required,number,gt=0"`
	PARENT_FILE_ID int64 `json:"parent_file_id" validate:"required,number,gt=0"`
}

// Insert implementation of FileParents
//gocyclo:ignore
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
			return Error(err, LastInsertErrorCode, "", "dbs.fileparents.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.fileparents.Insert")
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
		if utils.VERBOSE > 1 {
			log.Println("unable to execute", stm, "error", err)
		}
	}

	// now we need to ensure that the parentage exists at block and dataset level too
	// for that we perform the following items:

	// get block name of this_file_id and call it thisBlockID
	stm = getSQL("blockid4fileid")
	if utils.VERBOSE > 0 {
		log.Printf("get block id for file id\n%s\n%+v", stm, r.THIS_FILE_ID)
	}
	var thisBlockID int64
	var thisBlockName string
	err = tx.QueryRow(stm, r.THIS_FILE_ID).Scan(&thisBlockID, &thisBlockName)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to execute", stm, "error", err)
		}
	}

	// get block name of parent_file_id and call it parentBlockID
	stm = getSQL("blockid4fileid")
	if utils.VERBOSE > 0 {
		log.Printf("get block id for fileid\n%s\n%+v", stm, r.PARENT_FILE_ID)
	}
	var parentBlockID int64
	var parentBlockName string
	err = tx.QueryRow(stm, r.PARENT_FILE_ID).Scan(&parentBlockID, &parentBlockName)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to execute", stm, "error", err)
		}
	}

	// get dataset id of thisBlockID and call it thisDatasetID
	stm = getSQL("datasetid4blockid")
	if utils.VERBOSE > 0 {
		log.Printf("get dataset id for block id\n%s\n%+v", stm, thisBlockID)
	}
	var thisDatasetID int64
	err = tx.QueryRow(stm, thisBlockID).Scan(&thisDatasetID)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to execute", stm, "error", err)
		}
	}

	// get dataset id of parentBlockID and call it parentDatasetID
	stm = getSQL("datasetid4blockid")
	if utils.VERBOSE > 0 {
		log.Printf("get dataset id for block id\n%s\n%+v", stm, parentBlockID)
	}
	var parentDatasetID int64
	err = tx.QueryRow(stm, parentBlockID).Scan(&parentDatasetID)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to execute", stm, "error", err)
		}
	}

	// insert relationship between block and parent block
	var tbid, pbid int64
	stm = getSQL("blockparents_ids")
	err = tx.QueryRow(stm, thisBlockID, parentBlockID).Scan(&tbid, &pbid)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to execute", stm, "error", err)
		}
	}
	if tbid == 0 && pbid == 0 { // there is no such ids in BlockParents table
		blockParents := BlockParents{THIS_BLOCK_ID: thisBlockID, PARENT_BLOCK_ID: parentBlockID}
		err = blockParents.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 0 {
				log.Printf("unable to insert block parents %+v using input fileparents record %+v, error %v", blockParents, r, err)
				log.Println("this block name", thisBlockName)
				log.Println("parent block name", parentBlockName)
			}
		}
	}

	// insert relationship between dataset and parent dataset
	datasetParents := DatasetParents{
		THIS_DATASET_ID:   thisDatasetID,
		PARENT_DATASET_ID: parentDatasetID}
	err = datasetParents.Insert(tx)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to insert dataset parents %+v using input fileparents record %+v, error %v", datasetParents, r, err)
		}
		return Error(err, InsertErrorCode, "", "dbs.fileparents.Insert")
	}

	return nil
}

// Validate implementation of FileParents
func (r *FileParents) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if r.THIS_FILE_ID == 0 {
		msg := "missing this_file_id"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.fileparents.Validate")
	}
	if r.PARENT_FILE_ID == 0 {
		msg := "missing parent_file_id"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.fileparents.Validate")
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
		return Error(err, ReaderErrorCode, "", "dbs.fileparents.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.fileparents.Decode")
	}
	return nil
}

// InsertFileParents DBS API is used by /fileparents end-point
// it accepts FileParentBlockRecord
func (a *API) InsertFileParents() error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "", "dbs.fileparents.InsertFileParents")
	}
	defer tx.Rollback()
	err = a.InsertFileParentsBlockTxt(tx)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to insert file parents", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.fileparents.InsertFileParents")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return Error(err, CommitErrorCode, "", "dbs.fileparents.InsertFileParents")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}

// FileParentBlockRecord represents file parent DBS record
type FileParentBlockRecord struct {
	BlockName         string    `json:"block_name"`
	ChildParentIDList [][]int64 `json:"child_parent_id_list"`
}

// InsertFileParentsBlockTxt DBS API
//gocyclo:ignore
func (a *API) InsertFileParentsBlockTxt(tx *sql.Tx) error {
	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.fileparents.InsertFileParentsBlockTxt")
	}

	var args []interface{}
	var conds []string

	var rec FileParentBlockRecord
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data as FileParentBlockRecord", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.fileparents.InsertFileParentsBlockTxt")
	}
	if utils.VERBOSE > 1 {
		log.Printf("Insert FileParentsBlock record %+v", rec)
	}

	if len(rec.ChildParentIDList) == 0 {
		msg := "InsertFileParentsBlock API requires child_parent_id_list"
		log.Println(msg)
		return Error(
			InvalidParamErr,
			ParametersErrorCode,
			msg,
			"dbs.fileparents.InsertFileParentsBlockTxt")
	}

	// obtain file parent ids for a given block name
	cond := fmt.Sprintf(" B.BLOCK_NAME = %s", placeholder("block_name"))
	conds = append(conds, cond)
	args = append(args, rec.BlockName)
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
		log.Println(msg)
		return Error(err, QueryErrorCode, "", "dbs.fileparents.InsertFileParentsBlockTxt")
	}
	defer rows.Close()
	var bfids []int64
	for rows.Next() {
		var fid int64
		if err := rows.Scan(&fid); err != nil {
			log.Println("fail to get row.Scan, error", err)
			return Error(err, RowsScanErrorCode, "", "dbs.fileparents.InsertFileParentsBlockTxt")
		}
		bfids = append(bfids, fid)
	}

	// check that out file ids from block are the same as from child_parent_id_list
	var fids []int64
	for _, item := range rec.ChildParentIDList {
		fids = append(fids, item[0])
	}
	if utils.VERBOSE > 1 {
		log.Println("InsertFileParentsBlock fids", fids, "bfids", bfids)
	}
	if !utils.Equal(utils.OrderedSet(fids), utils.OrderedSet(bfids)) {
		log.Println("block fids != file ids")
		log.Println("block ids", bfids)
		log.Println("file  ids", fids)
		msg := fmt.Sprintf("not all files present in block")
		return Error(RecordErr, ParametersErrorCode, msg, "dbs.fileparents.InsertFileParentsBlockTxt")
	}

	// now we can loop over provided list and insert file parents
	for _, v := range rec.ChildParentIDList {
		var r FileParents
		r.THIS_FILE_ID = v[0]
		r.PARENT_FILE_ID = v[1]
		if utils.VERBOSE > 1 {
			log.Println("InsertFileParentsBlock", r)
		}
		err = r.Validate()
		if err != nil {
			log.Println("unable to validate the record", r, "error", err)
			return Error(err, ValidateErrorCode, "", "dbs.fileparents.InsertFileParentsBlockTxt")
		}
		err = r.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert FileParentsBlock record, error", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.fileparents.InsertFileParentsBlockTxt")
		}
	}
	return nil
}

// FileParentRecord represents file parent DBS record
// used by bulkblocks API
// NOTE: bulkblocks API should return this_logical_file_name as it is used by DBS migrate
// while users, e.g. CRAB, can construct by themselves the bulkblock structure where
// they may use logical_file_name name
// Therefore, we should keep both this_logical_file_name and logical_file_name
// together for backward compatibility
type FileParentRecord struct {
	ThisLogicalFileName   string `json:"this_logical_file_name,omitempty"`
	LogicalFileName       string `json:"logical_file_name,omitempty"`
	ParentLogicalFileName string `json:"parent_logical_file_name"`
}
