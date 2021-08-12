package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
)

// BlockDumpRecord represents input block record used in BlockDump and InsertBlockDump APIs
type BlockDumpRecord struct {
	BLOCK_ID            int64    `json:"block_id"`
	BLOCK_NAME          string   `json:"block_name"`
	DATASET             string   `json:"dataset"`
	PRIMARY_DATASET     string   `json:"prim_ds"`
	FILES               []string `json:"files"`
	BLOCK_PARENT_LIST   string   `json:"block_parent_list"`
	DATASET_PARENT_LIST string   `json:"dataset_parent_list"`
	FILE_CONF_LIST      string   `json:"file_conf_list"`
	FILE_PARENT_LIST    string   `json:"file_parent_list"`
	DATASET_CONF_LIST   string   `json:"dataset_conf_list"`
}

// TODO: see dumpBlock function in
// ../../Server/Python/src/dbs/business/DBSBlock.py (blockDump)
// ../../Server/Python/src/dbs/business/DBSBlockInsert.py (putBlock)
/*
The BlockDump python API returns the following dict
   result = dict(block=block, dataset=dataset, primds=primds,
                 files=files, block_parent_list=bparent,
                 ds_parent_list=dsparent, file_conf_list=fconfig_list,
                 file_parent_list=fparent_list2, dataset_conf_list=dconfig_list)
*/

// BlockDump DBS API
func (a *API) BlockDump() error {

	blk, err := getSingleValue(a.Params, "block_name")
	if err != nil {
		return err
	}
	// initialize BlockDumpRecord
	rec := BlockDumpRecord{BLOCK_NAME: blk}

	// fill out BlockDumpRecord details, see dumpBlock method in
	// ../../Server/Python/src/dbs/business/DBSBlock.py
	// - obtain dataset
	// - get dataset and block parentage
	// - get file parent list for given block_id
	// - get primary dataset
	// - get file/dataset conf list

	// write BlockDumpRecord
	data, err := json.Marshal(rec)
	if err == nil {
		a.Writer.Write(data)
	}
	return err
}

// InsertBlockDump insert block dump record into DBS
func (r *BlockDumpRecord) InsertBlockDump() error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	var tid int64
	if r.BLOCK_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "BLOCKS", "block_id")
			r.BLOCK_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_BK")
			r.BLOCK_ID = tid
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
	// logic of insertion
	// - insert dataset_conf_list
	// - insert dataset
	// - insert block
	// - insert files
	// - insert file lumis
	// - insert file configuration
	// - insert block and dataset parentage

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return err
	}
	return err
}

// Validate implementation of Blocks
func (r *BlockDumpRecord) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("block", r.BLOCK_NAME); err != nil {
		return err
	}
	if strings.Contains(r.BLOCK_NAME, "*") || strings.Contains(r.BLOCK_NAME, "%") {
		return errors.New("block name contains pattern")
	}
	return nil
}

// SetDefaults implements set defaults for Blocks
func (r *BlockDumpRecord) SetDefaults() {
}

// Decode implementation for Blocks
func (r *BlockDumpRecord) Decode(reader io.Reader) error {
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
