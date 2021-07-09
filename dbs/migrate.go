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
	"strings"
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
func prepareBlockMigrationList(url, block string) []string {
	/*
		1. see if block already exists at dst (no need to migrate),
		   raise "ALREADY EXISTS"
		2. see if block exists at src & make sure the block's open_for_writing=0
		3. see if block has parents
		4. see if parent blocks are already at dst
		5. add 'order' to parent and then this block (ascending)
		6. return the ordered list
	*/
	var out []string
	return out
}

// helper function to prepare the ordered lists of blocks based on input DATASET
func prepareDatasetMigrationList(url, dataset string) []string {
	/*
		1. Get list of blocks from source
		   - for a given dataset get list of blocks from local DB and remote url
		2. Check and see if these blocks are already at DST
		3. Check if dataset has parents
		4. Check if parent blocks are already at DST
	*/
	var out []string
	return out
}

// Submit DBS API
func (API) Submit(r io.Reader, cby string) error {
	/* Logic of submit API:
	- check if migration_input is already queued
	  - if already queued it should return migration_status
	  - if not prepare ordered list of dataset or block to migrate
	- iterate over ordered list of datasets or blocks
	  - prepare and insert MigrationBlocks object
	- return MigrationReport object
	*/
	// read given input
	data, err := ioutil.ReadAll(r)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := MigrationRequests{CREATE_BY: cby, LAST_MODIFIED_BY: cby}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}

	// set migration record
	mrec := MigrationRequests{MIGRATION_URL: rec.MIGRATION_URL, MIGRATION_INPUT: rec.MIGRATION_INPUT, MIGRATION_STATUS: rec.MIGRATION_STATUS, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY, LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE, LAST_MODIFIED_BY: rec.LAST_MODIFIED_BY}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	err = mrec.Insert(tx)
	if err != nil {
		return err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return err
	}
	return err
}

// Remove DBS API
func (API) Remove(r io.Reader, cby string) error {
	return nil
}

// Status DBS API
func (API) Status(params Record, w http.ResponseWriter) (int64, error) {
	return 0, nil
}
