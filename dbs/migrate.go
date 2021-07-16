package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/vkuznet/dbs2go/utils"
)

/*
DBS Migration APIs, see Python counterpart here:
Server/Python/src/dbs/web/DBSMigrateModel.py
Server/Python/src/dbs/business/DBSMigrate.py
and various bisuness dao, e.g.
Server/Python/src/dbs/dao/Oracle/MigrationBlock

Submit should submit migration request
(see insertMigrationRequest python API)

Status checks migration request
(see listMigrationRequests python API)

Remove removes migration request
(see removeMigrationRequest API)
*/

// MigrationReport represents migration report returned by the migration API
type MigrationReport struct {
	Report  string `json:"report"`
	Status  int    `json:"status"`
	Details string `json:"details"`
}

// helper function to get blocks from remote DBS server (remote url)
// the val parameter can be either dataset or block name
// it return list of blocks obtained from blocks API
func getBlocks(rurl, val string) ([]string, error) {
	var out []string
	if strings.Contains(val, "#") {
		rurl = fmt.Sprintf("%s/blocks?block_name=%s&open_for_writing=0", rurl, url.QueryEscape(val))
	} else {
		rurl = fmt.Sprintf("%s/blocks?dataset=%s&open_for_writing=0", rurl, val)
	}
	data, err := getData(rurl)
	if err != nil {
		return out, err
	}
	var rec []Blocks
	err = json.Unmarshal(data, &rec)
	if err != nil {
		return out, err
	}
	for _, v := range rec {
		out = append(out, v.BLOCK_NAME)
	}
	return out, nil
}

// helper function to prepare the ordered lists of blocks based on input BLOCK
// return map of blocks with their parents
func prepareBlockMigrationList(rurl, block string) (map[int][]string, error) {
	/*
		1. see if block already exists at dst (no need to migrate),
		   raise "ALREADY EXISTS"
		2. see if block exists at src & make sure the block's open_for_writing=0
		3. see if block has parents
		4. see if parent blocks are already at dst
		5. add 'order' to parent and then this block (ascending)
		6. return the ordered list
	*/
	var out map[int][]string

	// check if block exists at destination (this server)
	localhost := utils.BasePath(utils.BASE, "/blocks")
	dstblocks, err := getBlocks(localhost, block)
	if err != nil {
		return out, err
	}
	if len(dstblocks) > 0 {
		msg := fmt.Sprintf("requested blocks %v is already at destination", dstblocks)
		return out, errors.New(msg)
	}

	// check if block exists at a source location
	srcblocks, err := getBlocks(rurl, block)
	if err != nil {
		return out, err
	}
	if len(srcblocks) == 0 {
		msg := fmt.Sprintf("requested block %s is not found at %s", block, rurl)
		return out, errors.New(msg)
	}
	// we need to migrate existing block
	var blocks []string
	blocks = append(blocks, block)
	out[0] = blocks
	parentBlocks, err := getParentBlocksOrderedList(rurl, block)
	if err != nil {
		return out, err
	}
	for idx, blks := range parentBlocks {
		out[idx] = blks
	}
	return out, nil
}

// helper function to get parent blocks ordered list for given url and block name
func getParentBlocksOrderedList(rurl, block string) (map[int][]string, error) {
	var out map[int][]string
	return out, nil
}

// helper function to prepare the ordered lists of blocks based on input DATASET
// return map of blocks with their parents
func prepareDatasetMigrationList(rurl, dataset string) (map[int][]string, error) {
	/*
		1. Get list of blocks from source
		   - for a given dataset get list of blocks from local DB and remote url
		2. Check and see if these blocks are already at DST
		3. Check if dataset has parents
		4. Check if parent blocks are already at DST
	*/
	var out map[int][]string
	return out, nil
}

// helper function to check if migration is already queued
func alreadyQueued(input string, w http.ResponseWriter) error {
	report := MigrationReport{}
	data, err := json.Marshal(report)
	if err == nil {
		w.Write(data)
	}
	return err
}

// helper function to write Migration Report to http response writer and return its error to upstream caller
func writeReport(msg string, err error, w http.ResponseWriter) error {
	report := MigrationReport{Report: msg, Details: fmt.Sprintf("%v", err)}
	log.Println(msg, err)
	if data, e := json.Marshal(report); e == nil {
		w.Write(data)
	}
	return err
}

// Submit DBS API
func (API) Submit(r io.Reader, cby string, w http.ResponseWriter) error {
	/* Logic of submit API:
	- check if migration_input is already queued
	  - if already queued it should return migration_status
	  - if not prepare ordered list of dataset or block to migrate
	- iterate over ordered list of datasets or blocks
	  - prepare and insert MigrationBlocks object
	- write MigrationReport object
	*/

	// read given input
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return writeReport("fail to read data", err, w)
	}
	rec := MigrationRequests{CREATE_BY: cby, LAST_MODIFIED_BY: cby}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		return writeReport("fail to decode data", err, w)
	}

	// check if migration input is already queued
	input := rec.MIGRATION_INPUT
	if err := alreadyQueued(input, w); err != nil {
		return err
	}
	var migBlocks map[int][]string
	rurl := rec.MIGRATION_URL
	if strings.Contains(input, "#") {
		migBlocks, err = prepareBlockMigrationList(rurl, input)
	} else {
		migBlocks, err = prepareDatasetMigrationList(rurl, input)
	}
	if err != nil {
		return err
	}

	var orderedList []int
	for k, _ := range migBlocks {
		orderedList = append(orderedList, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(orderedList)))

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return writeReport("unable to get DB transaction", err, w)
	}
	defer tx.Rollback()

	// insert MigrationRequest object
	err = rec.Insert(tx)
	if err != nil {
		return writeReport("fail to insert MigrationBlocks record", err, w)
	}

	// loop over orderedList which is [[blocks], [blocks]]
	// and insert every chunk of blocks as MigrationBlocks objects
	var totalQueued int
	for idx, b := range orderedList {
		for _, blk := range migBlocks[b] {
			// set migration record
			mrec := MigrationBlocks{
				MIGRATION_STATUS:       rec.MIGRATION_STATUS,
				MIGRATION_ORDER:        int64(idx),
				MIGRATION_BLOCK_NAME:   blk,
				CREATION_DATE:          rec.CREATION_DATE,
				CREATE_BY:              rec.CREATE_BY,
				LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE,
				LAST_MODIFIED_BY:       rec.LAST_MODIFIED_BY}
			err = mrec.Insert(tx)
			if err != nil {
				return writeReport("fail to insert MigrationBlocks record", err, w)
			}
			totalQueued += 1
		}
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		return writeReport("fail to commit transaction", err, w)
	}
	report := MigrationReport{Report: fmt.Sprintf("REQUEST QUEUED with total %d blocks to be migrated", totalQueued), Details: string(data)}
	data, err = json.Marshal(report)
	if err == nil {
		w.Write(data)
	}
	return err
}

// MigrationRemoveRequest represents migration remove request object
type MigrationRemoveRequest struct {
	MIGRATION_REQUEST_ID int64  `json:"migration_rqst_id"`
	CREATE_BY            string `json:"create_by"`
}

// Remove DBS API
func (API) Remove(r io.Reader, cby string, w http.ResponseWriter) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return writeReport("fail to read data", err, w)
	}
	rec := MigrationRemoveRequest{CREATE_BY: cby}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		return writeReport("fail to decode data", err, w)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return writeReport("unable to get DB transaction", err, w)
	}
	defer tx.Rollback()

	stm := getSQL("count_migration_requests")
	var tid float64
	err = tx.QueryRow(stm, rec.MIGRATION_REQUEST_ID, rec.CREATE_BY).Scan(&tid)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
		return writeReport(msg, err, w)
	}

	if tid > 1 {
		stm = getSQL("remove_migration_requests")
		_, err = tx.Exec(stm, rec.MIGRATION_REQUEST_ID, rec.CREATE_BY)
		err = tx.Commit()
		if err != nil {
			return writeReport("fail to commit transaction", err, w)
		}
	}
	return nil
}

// MigrationStatusRequest defines status request structure
type MigrationStatusRequest struct {
	BLOCK_NAME string `json:"block_name"`
	DATASET    string `json:"dataset"`
	USER       string `json:"user"`
}

// Status DBS API
func (API) Status(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	oldest, _ := getSingleValue(params, "oldest")
	if oldest == "true" {
		tmpl["Oldest"] = true
	}
	if _, e := getSingleValue(params, "migration_request_id"); e == nil {
		conds, args = AddParam("migration_request_id", "MR.MIGRATION_REQUEST_ID", params, conds, args)
	}
	if _, e := getSingleValue(params, "migration_input"); e == nil {
		conds, args = AddParam("migration_input", "MR.MIGRATION_INPUT", params, conds, args)
	}
	if _, e := getSingleValue(params, "migration_url"); e == nil {
		conds, args = AddParam("migration_url", "MR.MIGRATION_URL", params, conds, args)
	}
	if _, e := getSingleValue(params, "dataset"); e == nil {
		conds, args = AddParam("dataset", "MR.DATASET", params, conds, args)
	}
	if _, e := getSingleValue(params, "block_name"); e == nil {
		conds, args = AddParam("block_name", "MR.BLOCK_NAME", params, conds, args)
	}
	if _, e := getSingleValue(params, "user"); e == nil {
		conds, args = AddParam("user", "MR.USER", params, conds, args)
	}
	if _, e := getSingleValue(params, "create_by"); e == nil {
		conds, args = AddParam("create_by", "MR.CREATE_BY", params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("migration_requests")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
