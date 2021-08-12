package dbs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

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

DBS migration status codes:
        migration_status:
        0=PENDING
        1=IN PROGRESS
        2=COMPLETED
        3=FAILED (will be retried)
        9=Terminally FAILED
        status change:
        0 -> 1
        1 -> 2
        1 -> 3
        1 -> 9
        are only allowed changes for working through migration.
        3 -> 1 is allowed for retrying and retry count +1.
*/

// MigrationCodes represents all migration codes
const (
	PENDING = iota
	IN_PROGRESS
	COMPLETED
	FAILED
	TERM_FAILED
)

// MigrationProcessTimeout defines migration process timeout
var MigrationProcessTimeout int

// MigrationReport represents migration report returned by the migration API
type MigrationReport struct {
	Report  string `json:"report"`
	Status  int    `json:"status"`
	Details string `json:"details"`
}

// GetBlocks returns list of blocks for a given url and block/dataset input
func GetBlocks(rurl, val string) ([]string, error) {
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

// GetParents returns list of parents for given block or dataset
func GetParents(rurl, val string) ([]string, error) {
	var out []string
	if strings.Contains(val, "#") {
		rurl = fmt.Sprintf("%s/blockparents?block_name=%s", rurl, val)
	} else {
		rurl = fmt.Sprintf("%s/datasetparents?dataset=%s", rurl, val)
	}
	data, err := getData(rurl)
	if err != nil {
		return out, err
	}
	var rec []map[string]interface{}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		return out, err
	}
	for _, v := range rec {
		if strings.Contains(val, "#") {
			block := fmt.Sprintf("%v", v["parent_block_name"])
			out = append(out, block)
		} else {
			dataset := fmt.Sprintf("%v", v["parent_dataset"])
			out = append(out, dataset)
		}
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
	dstblocks, err := GetBlocks(localhost, block)
	if err != nil {
		return out, err
	}
	if len(dstblocks) > 0 {
		msg := fmt.Sprintf("requested blocks %v is already at destination", dstblocks)
		return out, errors.New(msg)
	}

	// check if block exists at a source location
	srcblocks, err := GetBlocks(rurl, block)
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
	orderCounter := 0
	out[orderCounter] = blocks
	parentBlocks, err := GetParentBlocks(rurl, block, orderCounter)
	if err != nil {
		return out, err
	}
	for idx, blks := range parentBlocks {
		out[idx] = blks
	}
	return out, nil
}

// BlockResponse represents block response structure used in GetParentBlocks
type BlockResponse struct {
	Dataset string
	Blocks  []string
	Error   error
}

// GetParentBlocks returns parent blocks ordered list for given url and block name
func GetParentBlocks(rurl, block string, orderCounter int) (map[int][]string, error) {
	out := make(map[int][]string)
	// get list of blocks from the source (remote url)
	srcblocks, err := GetBlocks(rurl, block)
	if err != nil {
		log.Println("unable to get list of blocks at remote url", rurl, err)
		return out, err
	}
	// get list of parent blocks at destination (this server)
	parentBlocksInDst := make(map[string]bool)
	localhost := utils.BasePath(utils.BASE, "/blocks")
	ch := make(chan BlockResponse)
	umap := make(map[string]struct{})
	for _, blk := range srcblocks {
		dataset := strings.Split(blk, "#")[0]
		umap[dataset] = struct{}{}
		go func() {
			blks, err := GetBlocks(localhost, dataset)
			ch <- BlockResponse{Dataset: dataset, Blocks: blks, Error: err}
		}()
	}
	// collect results from goroutines
	for {
		select {
		case r := <-ch:
			if r.Error != nil {
				log.Printf("unable to fetch blocks for url=%s dataset=%s error=%v", localhost, r.Dataset, r.Error)
			} else {
				for _, blk := range r.Blocks {
					parentBlocksInDst[blk] = true
				}
			}
			delete(umap, r.Dataset)
		default:
			if len(umap) == 0 {
				break
			}
			time.Sleep(time.Duration(1) * time.Millisecond) // wait for response
		}
	}

	// loop over source blocks
	for _, blk := range srcblocks {
		if _, ok := parentBlocksInDst[blk]; !ok {
			// block is not at destination
			if list, ok := out[orderCounter]; ok {
				list = append(list, blk)
				out[orderCounter] = list
			} else {
				out[orderCounter] = []string{blk}
			}
			omap, err := GetParentBlocks(rurl, blk, orderCounter+1)
			if err != nil {
				log.Printf("fail to get url=%s block=%s error=%v", rurl, blk, err)
				continue
			}
			out = utils.UpdateOrderedDict(out, omap)
		}
	}
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
	orderCounter := 0
	out, err := processDatasetBlocks(rurl, dataset, orderCounter)
	if err != nil {
		return out, err
	}
	if len(out) == 0 {
		msg := fmt.Sprintf("requested dataset %s is already at destination", dataset)
		return out, errors.New(msg)
	}
	pdict, err := GetParentDatasets(rurl, dataset, orderCounter+1)
	if err != nil {
		return out, err
	}
	if len(pdict) != 0 {
		// update out
		out = utils.UpdateOrderedDict(out, pdict)
	}
	return out, nil
}

// helper function, that comapares blocks of a dataset at source and dst
// and returns an ordered list of blocks not already at dst for migration
func processDatasetBlocks(rurl, dataset string, orderCounter int) (map[int][]string, error) {
	out := make(map[int][]string)
	srcblks, err := GetBlocks(rurl, dataset)
	if err != nil {
		return out, err
	}
	if len(srcblks) == 0 {
		msg := fmt.Sprintf("No blocks in the required dataset %s found at source %s", dataset, rurl)
		return out, errors.New(msg)
	}
	localhost := utils.BasePath(utils.BASE, "/blocks")
	dstblks, err := GetBlocks(localhost, dataset)
	if err != nil {
		return out, err
	}
	dstBlocksMap := make(map[string]struct{})
	for _, blk := range dstblks {
		dstBlocksMap[blk] = struct{}{}
	}
	for idx, blk := range srcblks {
		if _, ok := dstBlocksMap[blk]; !ok {
			if eblks, ok := out[idx]; ok {
				eblks = append(eblks, blk)
				out[idx] = eblks
			} else {
				out[idx] = []string{blk}
			}
		}
	}
	return out, nil
}

// DatasetResponse represents response of processDatasetBlocks API
type DatasetResponse struct {
	Dataset    string
	OrderedMap map[int][]string
	Error      error
}

// GetParentDatasets return ordered dict of parent datasets
func GetParentDatasets(rurl, dataset string, orderCounter int) (map[int][]string, error) {
	out := make(map[int][]string)
	parentDatasets, err := GetParents(rurl, dataset)
	if err != nil {
		return out, err
	}
	ch := make(chan DatasetResponse)
	umap := make(map[string]struct{})
	for _, dataset := range parentDatasets {
		umap[dataset] = struct{}{}
		go func() {
			omap, err := processDatasetBlocks(rurl, dataset, orderCounter)
			// get ordered map of parents
			pmap, err := GetParentDatasets(rurl, dataset, orderCounter+1)
			if err == nil && len(pmap) > 0 {
				omap = utils.UpdateOrderedDict(omap, pmap)
			}
			ch <- DatasetResponse{Dataset: dataset, OrderedMap: omap, Error: err}
		}()
	}
	// collect results from goroutines
	for {
		select {
		case r := <-ch:
			if r.Error != nil {
				log.Printf("unable to fetch blocks for url=%s dataset=%s error=%v", rurl, r.Dataset, r.Error)
			} else {
				out = utils.UpdateOrderedDict(out, r.OrderedMap)
			}
			delete(umap, r.Dataset)
		default:
			if len(umap) == 0 {
				break
			}
			time.Sleep(time.Duration(1) * time.Millisecond) // wait for response
		}
	}

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

// SubmitMigration DBS API
func (a *API) SubmitMigration() error {
	/* Logic of submit API:
	- check if migration_input is already queued
	  - if already queued it should return migration_status
	  - if not prepare ordered list of dataset or block to migrate
	- iterate over ordered list of datasets or blocks
	  - prepare and insert MigrationBlocks object
	- write MigrationReport object
	- spawn goroutine to process migration report
	*/

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		return writeReport("fail to read data", err, a.Writer)
	}
	rec := MigrationRequests{CREATE_BY: a.CreateBy, LAST_MODIFIED_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		return writeReport("fail to decode data", err, a.Writer)
	}

	// check if migration input is already queued
	input := rec.MIGRATION_INPUT
	if err := alreadyQueued(input, a.Writer); err != nil {
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
		return writeReport("unable to get DB transaction", err, a.Writer)
	}
	defer tx.Rollback()

	// insert MigrationRequest object
	err = rec.Insert(tx)
	if err != nil {
		return writeReport("fail to insert MigrationBlocks record", err, a.Writer)
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
				return writeReport("fail to insert MigrationBlocks record", err, a.Writer)
			}
			totalQueued += 1
		}
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		return writeReport("fail to commit transaction", err, a.Writer)
	}

	report := MigrationReport{Report: fmt.Sprintf("REQUEST QUEUED with total %d blocks to be migrated", totalQueued), Details: string(data)}
	data, err = json.Marshal(report)
	if err == nil {
		a.Writer.Write(data)
	}

	// once migration report is ready we'll process it asynchronously
	a.Params["migration_request_url"] = rurl
	go a.ProcessMigration(false) // do not write process report

	return err
}

// ProcessMigration will process given migration request
// and inject data to source DBS
func (a *API) ProcessMigration(writeReport bool) error {

	// setup context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(MigrationProcessTimeout)*time.Second)
	defer cancel()

	// create channel to report when operation will be completed
	ch := make(chan bool)

	// execute slow operation in background
	go a.processMigration(ch)

	// the slow operation will either finish or timeout
	var status int
	var err error
	var msg string
	select {
	case <-ctx.Done():
		msg = fmt.Sprintf("Process migration function timeout")
		err = errors.New(msg)
		status = FAILED
	case <-ch:
		msg = fmt.Sprintf("Process migration successful")
		status = COMPLETED
	}
	report := MigrationReport{Report: msg, Status: status}
	log.Println(report.Report)
	if writeReport {
		data, err := json.Marshal(report)
		if err == nil {
			a.Writer.Write(data)
		}
		return err
	}
	return err
}

// processMigration will process given migration report
// and inject data to source DBS
func (a *API) processMigration(ch chan<- bool) {
	murl, ok := a.Params["migration_request_url"]
	if !ok {
		log.Println("unable to get migration_request_url")
		ch <- true
	}

	// obtain block details from destination DBS
	rurl := fmt.Sprintf("%s/blockdump", murl)
	data, err := getData(rurl)
	if err != nil {
		log.Printf("unable to query %s/blockdump, error %v", rurl, err)
	}
	var rec BlockDumpRecord
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Printf("unable to unmarshal BlockDumpRecord, error %v", err)
	}

	// update migration status
	a.UpdateMigrationStatus()

	// insert block dump record into source DBS
	err = rec.InsertBlockDump()
	if err != nil {
		log.Println("insert block dump record failed with", err)
	}
	// report when we done
	ch <- true
}

// UpdateMigrationStatus updates migration status
func (a *API) UpdateMigrationStatus() {
}

// MigrationRemoveRequest represents migration remove request object
type MigrationRemoveRequest struct {
	MIGRATION_REQUEST_ID int64  `json:"migration_rqst_id"`
	CREATE_BY            string `json:"create_by"`
}

// RemoveMigration DBS API
func (a *API) RemoveMigration() error {
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		return writeReport("fail to read data", err, a.Writer)
	}
	rec := MigrationRemoveRequest{CREATE_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		return writeReport("fail to decode data", err, a.Writer)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return writeReport("unable to get DB transaction", err, a.Writer)
	}
	defer tx.Rollback()

	stm := getSQL("count_migration_requests")
	var tid float64
	err = tx.QueryRow(stm, rec.MIGRATION_REQUEST_ID, rec.CREATE_BY).Scan(&tid)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
		return writeReport(msg, err, a.Writer)
	}

	if tid > 1 {
		stm = getSQL("remove_migration_requests")
		_, err = tx.Exec(stm, rec.MIGRATION_REQUEST_ID, rec.CREATE_BY)
		err = tx.Commit()
		if err != nil {
			return writeReport("fail to commit transaction", err, a.Writer)
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

// StatusMigration DBS API
func (a *API) StatusMigration() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	oldest, _ := getSingleValue(a.Params, "oldest")
	if oldest == "true" {
		tmpl["Oldest"] = true
	}
	if _, e := getSingleValue(a.Params, "migration_request_id"); e == nil {
		conds, args = AddParam("migration_request_id", "MR.MIGRATION_REQUEST_ID", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "migration_input"); e == nil {
		conds, args = AddParam("migration_input", "MR.MIGRATION_INPUT", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "migration_url"); e == nil {
		conds, args = AddParam("migration_url", "MR.MIGRATION_URL", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "dataset"); e == nil {
		conds, args = AddParam("dataset", "MR.DATASET", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "block_name"); e == nil {
		conds, args = AddParam("block_name", "MR.BLOCK_NAME", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "user"); e == nil {
		conds, args = AddParam("user", "MR.USER", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "create_by"); e == nil {
		conds, args = AddParam("create_by", "MR.CREATE_BY", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("migration_requests")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}
