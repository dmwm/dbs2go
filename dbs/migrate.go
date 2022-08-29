package dbs

// DBS Migrate APIS
// Copyright (c) 2021 - Valentin Kuznetsov <vkuznet@gmail.com>
//
// DBS Migration service is responsible to migrate blocks from one
// DBS to another. This module provides the following APIs:
// - Submit to submit migration request, internall it prepare the request
// and calls via goroutine process request
// - Process to process migration request explicitly
// - Remove to remove migration request
// - Status to obtain status of migration request
// Internally the migration process injects all request details into
// MigrationRequest table. The request details resides in MigrationBlocks table.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/dbs2go/utils"
)

/*

DBS Migration APIs:

- submit should submit migration request
- status checks migration request
- remove removes migration request
- process processes migration requests
- totat reports number of migration requests
- cleanup clean migration DB

DBS migration status codes (in sync with old python migration server):
        0=PENDING
        1=IN PROGRESS
        2=COMPLETED
        3=FAILED (will be retried)
		4=EXIST_IN_DB
		5=QUEUED
        9=Terminally FAILED
        status change:
		QUEUED -> PENDING (5 -> 0)
        PENDING -> IN PROGRESS (0 -> 1)
        IN PROGRESS -> COMPLETED (1 -> 2)
        IN PROGRESS -> FAILED (1 -> 3)
		IN PROGRESS -> EXIST_IN_DB (1 -> 4)
        IN PROGRESS -> (Terminally FAILED) (1 -> 9)
        are only allowed changes for working through migration.
        FAILED -> IN PROGRESS (3 -> 1) is allowed for retrying and retry count +1.
*/

// MigrationAsyncTimeout defines timeout of asynchrounous migration request process
var MigrationAsyncTimeout int

// MigrationCodes represents all migration codes
const (
	PENDING = iota
	IN_PROGRESS
	COMPLETED
	FAILED
	EXIST_IN_DB
	QUEUED
	TERM_FAILED = 9
)

// MigrateURL holds URL of DBSMigrate server
var MigrateURL string

// MigrationReport represents migration report returned by the migration API
type MigrationReport struct {
	MigrationRequest MigrationRequest `json:"migration_details"`
	Report           string           `json:"migration_report"`
	Status           string           `json:"status"`
	Error            error            `json:"error"`
}

// GetBlocks returns list of blocks for a given url and block/dataset input
func GetBlocks(rurl, val string) ([]string, error) {
	var out []string
	open := "&open_for_writing=0"
	if strings.Contains(val, "#") {
		rurl = fmt.Sprintf("%s/blocks?block_name=%s%s", rurl, url.QueryEscape(val), open)
	} else {
		rurl = fmt.Sprintf("%s/blocks?dataset=%s%s", rurl, val, open)
	}
	data, err := getData(rurl)
	if utils.VERBOSE > 0 {
		log.Println("GetBlocks", rurl, string(data))
	}
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to get data for %s, error %v", rurl, err)
		}
		return out, Error(err, HttpRequestErrorCode, "", "dbs.migrate.GetBlocks")
	}
	var rec []Blocks
	err = json.Unmarshal(data, &rec)
	if err != nil {
		return out, Error(err, UnmarshalErrorCode, "", "dbs.migrate.GetBlocks")
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
		rurl = fmt.Sprintf("%s/blockparents?block_name=%s", rurl, url.QueryEscape(val))
	} else {
		rurl = fmt.Sprintf("%s/datasetparents?dataset=%s", rurl, val)
	}
	data, err := getData(rurl)
	if err != nil {
		return out, Error(err, HttpRequestErrorCode, "", "dbs.migrate.GetParents")
	}
	var rec []map[string]interface{}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Printf("unable to unmarshal data url=%s data=%s error=%v", rurl, string(data), err)
		return out, Error(err, UnmarshalErrorCode, "", "dbs.migrate.GetParents")
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

// get list of migration blocks in order of processing (first parents then children)
func GetMigrationBlocksInOrder(mblocks []MigrationBlock) []string {
	if utils.VERBOSE > 1 {
		log.Println("GetMigrationBlocksInOrder len(mblocks)", len(mblocks))
		for _, r := range mblocks {
			log.Printf("Migration block %+v", r)
		}
	}
	bdict := make(map[int][]string)
	var olist []int
	for _, r := range mblocks {
		olist = append(olist, r.Order)
		if blocks, ok := bdict[r.Order]; ok {
			blocks = append(blocks, r.Block)
			bdict[r.Order] = blocks
		} else {
			bdict[r.Order] = []string{r.Block}
		}
	}
	var blocks []string
	orders := utils.Set(olist)
	sort.Ints(orders)
	for _, o := range orders {
		for _, blk := range bdict[o] {
			blocks = append(blocks, blk)
		}
	}
	return blocks
}

// helper function to prepare the list of parent blocks for given input
func prepareMigrationList(rurl, input string) []string {
	time0 := time.Now()
	var pblocks []string
	var mblocks []MigrationBlock
	var err error
	if utils.VERBOSE > 0 {
		log.Println("prepare migration list", rurl, input)
	}
	order := 0 // migration order
	if strings.Contains(input, "#") {
		mblocks, err = GetParentBlocks(rurl, input, order)
		pblocks = GetMigrationBlocksInOrder(mblocks)
		if len(pblocks) == 0 {
			pblocks = append(pblocks, input)
		}
	} else {
		mblocks, err = GetParentDatasetBlocks(rurl, input, order)
		pblocks = GetMigrationBlocksInOrder(mblocks)
		// if no parents exist for given dataset we'll find its blocks
		if len(pblocks) == 0 {
			blocks, err := processDatasetBlocks(rurl, input)
			if err == nil {
				pblocks = blocks
			} else {
				if utils.VERBOSE > 1 {
					log.Printf("unable to find blocks from %s for %s, error %v", rurl, input, err)
				}
			}
		}
	}
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Printf("unable to find parent blocks from %s for %s, error %v", rurl, input, err)
		}
		return pblocks
	}
	if utils.VERBOSE > 1 {
		log.Printf("prepareMigrationList yields %d blocks from %s for %s, elapsed time %v", len(pblocks), rurl, input, time.Since(time0))
	}
	return pblocks
}

// helper function to check blocks in local DB
func blocksInDB(blocks []string) ([]string, error) {
	if len(blocks) == 0 {
		return blocks, nil
	}
	srcBlocks := []string{}
	hash := utils.GetHash([]byte(blocks[0]))
	tx, err := DB.Begin()
	if err != nil {
		return srcBlocks, Error(err, TransactionErrorCode, hash, "dbs.migrate.blocksInDB")
	}
	defer tx.Rollback()
	for _, blk := range blocks {
		if rid, err := GetID(tx, "BLOCKS", "block_id", "block_name", blk); err == nil && rid == 0 {
			srcBlocks = append(srcBlocks, blk)
		}
	}
	return srcBlocks, nil
}

// helper function to check blocks at source destination for provided
// blocks list
func prepareMigrationListAtSource(rurl string, blocks []string) []string {
	if strings.Contains(rurl, "localhost") {
		srcBlocks, err := blocksInDB(blocks)
		if err != nil {
			log.Println("WARNING: unable to get blocksInDB", err)
		} else {
			return srcBlocks
		}
	}
	// get list of parent blocks of previous parents
	srcBlocks := []string{}
	ch := make(chan BlockResponse)
	umap := make(map[int]struct{})
	for idx, blk := range blocks {
		umap[idx] = struct{}{}
		go func(i int, b string) {
			blks, err := GetBlocks(rurl, b)
			ch <- BlockResponse{Index: i, Block: b, Blocks: blks, Error: err}
		}(idx, blk)
	}
	if len(umap) == 0 {
		// no parent blocks
		if utils.VERBOSE > 1 {
			log.Printf("no blocks found %v in %s", blocks, rurl)
		}
		return srcBlocks
	}
	// collect results from goroutines
	exit := false
	for {
		select {
		case r := <-ch:
			if r.Error != nil {
				if utils.VERBOSE > 1 {
					log.Printf("unable to fetch blocks for url=%s block=%s error=%v", rurl, r.Block, r.Error)
				}
			} else {
				for _, blk := range r.Blocks {
					srcBlocks = append(srcBlocks, blk)
				}
			}
			delete(umap, r.Index)
		default:
			if len(umap) == 0 {
				exit = true
			}
			time.Sleep(time.Duration(100) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	return srcBlocks
}

// BlockResponse represents block response structure used in GetParentBlocks
type BlockResponse struct {
	Index   int
	Dataset string
	Block   string
	Blocks  []string
	Error   error
}

// MigrationBlock represent block with migration order
type MigrationBlock struct {
	Block string
	Order int
}

// GetParentBlocks returns parent blocks for given url and block name
//gocyclo:ignore
func GetParentBlocks(rurl, block string, order int) ([]MigrationBlock, error) {
	time0 := time.Now()

	if utils.VERBOSE > 1 {
		log.Printf("GetParentBlocks for %s order %d from %s", block, order, rurl)
	}
	out := []MigrationBlock{}
	if utils.VERBOSE > 1 {
		log.Println("call GetParentBlocks with", block)
	}
	// check if we got RAW dataset/block, if so return immediately
	arr := strings.Split(block, "#")
	if len(arr) > 0 {
		dataset := arr[0]
		if strings.HasSuffix(dataset, "/RAW") {
			return out, nil
		}
	}
	// when we insert given block in our migration blocks it should be last
	// to process as it is up in a hierarchy, therefore for it we use order+1
	out = append(out, MigrationBlock{Block: block, Order: order + 1})
	// get list of blocks from the source (remote url)
	//     srcblocks, err := GetBlocks(rurl, "blockparents", block)
	srcblocks, err := GetParents(rurl, block)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to get list of blocks at remote url", rurl, err)
		}
		return out, Error(err, HttpRequestErrorCode, "", "dbs.migrate.GetParentsBlock")
	}
	// add block parents to final list
	for _, blk := range srcblocks {
		out = append(out, MigrationBlock{Block: blk, Order: order})
	}
	if len(srcblocks) == 0 {
		// no parent blocks
		if utils.VERBOSE > 1 {
			log.Printf("no parent blocks found for %s in %s, elapsed time %v", block, rurl, time.Since(time0))
		}
		return out, nil
	}
	// get list of parent blocks of previous parents
	parentBlocks := []MigrationBlock{}
	ch := make(chan BlockResponse)
	umap := make(map[int]struct{})
	for idx, blk := range srcblocks {
		umap[idx] = struct{}{}
		go func(i int, b string) {
			blks, err := GetParents(rurl, b)
			ch <- BlockResponse{Index: i, Block: b, Blocks: blks, Error: err}
		}(idx, blk)
	}
	if len(umap) == 0 {
		// no parent blocks
		if utils.VERBOSE > 1 {
			log.Printf("no parent blocks found for %s in %s, elapsed time %v", block, rurl, time.Since(time0))
		}
		return out, nil
	}
	// collect results from goroutines
	exit := false
	for {
		select {
		case r := <-ch:
			if r.Error != nil {
				if utils.VERBOSE > 1 {
					log.Printf("unable to fetch blocks for url=%s block=%s error=%v", rurl, r.Block, r.Error)
				}
			} else {
				for _, blk := range r.Blocks {
					parentBlocks = append(parentBlocks, MigrationBlock{Block: blk, Order: order - 1})
				}
			}
			delete(umap, r.Index)
		default:
			if len(umap) == 0 {
				exit = true
			}
			time.Sleep(time.Duration(100) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}

	// loop over parent blocks and get its parents
	for _, pblk := range parentBlocks {
		out = append(out, pblk)
		// request parents of given block and decrease its order since
		// it will allow to process it before our block
		results, err := GetParentBlocks(rurl, pblk.Block, pblk.Order-2)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Printf("fail to get url=%s block=%v error=%v", rurl, pblk, err)
			}
			continue
		}
		for _, b := range results {
			out = append(out, b)
		}
	}

	if utils.VERBOSE > 1 {
		log.Printf("GetParentBlocks for %s yields %d block parents in %v", block, len(out), time.Since(time0))
	}
	return out, nil
}

// helper function, that comapares blocks of a dataset at source and dst
// and returns list of blocks not already at dst for migration
func processDatasetBlocks(rurl, dataset string) ([]string, error) {
	out := []string{}
	srcblks, err := GetBlocks(rurl, dataset)
	if err != nil {
		return out, Error(err, HttpRequestErrorCode, "", "dbs.migrate.processDatasetBlocks")
	}
	if len(srcblks) == 0 {
		msg := fmt.Sprintf("No blocks in the required dataset %s found at source %s", dataset, rurl)
		return out, Error(GenericErr, GenericErrorCode, msg, "dbs.migrate.processDatasetBlocks")
	}
	localhost := fmt.Sprintf("%s%s", utils.Localhost, utils.BASE)
	dstblks, err := GetBlocks(localhost, dataset)
	if err != nil {
		return srcblks, Error(err, HttpRequestErrorCode, "", "dbs.migrate.processDatasetBlocks")
	}
	for _, blk := range srcblks {
		if !utils.InList(blk, dstblks) {
			out = append(out, blk)
		}
	}
	return out, nil
}

// DatasetResponse represents response of processDatasetBlocks API
type DatasetResponse struct {
	Dataset         string
	MigrationBlocks []MigrationBlock
	Error           error
}

// GetParentDatasetBlocks returns full list of parent blocks associated with given dataset
//gocyclo:ignore
func GetParentDatasetBlocks(rurl, dataset string, order int) ([]MigrationBlock, error) {
	if utils.VERBOSE > 1 {
		log.Printf("GetParentDatasetBlocks for %s order %d from %s", dataset, order, rurl)
	}
	out := []MigrationBlock{}
	parentDatasets, err := GetParents(rurl, dataset)
	if err != nil {
		return out, Error(err, HttpRequestErrorCode, "", "dbs.migrate.GetParentDatasetBlocks")
	}
	if utils.VERBOSE > 1 {
		log.Printf("### for dataset %s we found parents datasets %v", dataset, parentDatasets)
	}
	ch := make(chan DatasetResponse)
	umap := make(map[string]struct{})
	for _, dataset := range parentDatasets {
		umap[dataset] = struct{}{}
		go func() {
			if utils.VERBOSE > 1 {
				log.Printf("processDatasetBlocks for %s order %d from %s", dataset, order, rurl)
			}
			blocks, err := processDatasetBlocks(rurl, dataset)
			if err != nil {
				if utils.VERBOSE > 1 {
					log.Println("unable to process dataset blocks", err)
				}
			}
			// get recursive list of parent blocks in reverse order
			pblocks, err := GetParentDatasetBlocks(rurl, dataset, order-1)
			if err != nil {
				if utils.VERBOSE > 1 {
					log.Println("unable to process parent dataset blocks", err)
				}
			}
			// add dataset blocks to list of our parent blocks
			for _, b := range blocks {
				pblocks = append(pblocks, MigrationBlock{Block: b, Order: order})
			}
			ch <- DatasetResponse{Dataset: dataset, MigrationBlocks: pblocks, Error: nil}
		}()
	}
	if len(umap) == 0 {
		// no parent datasets
		if utils.VERBOSE > 1 {
			log.Printf("no parent datasets found for %s in %s", dataset, rurl)
		}
		return out, nil
	}
	if utils.VERBOSE > 1 {
		log.Printf("process %d dataset", len(umap))
	}
	// collect results from goroutines
	exit := false
	for {
		select {
		case r := <-ch:
			if r.Error != nil {
				if utils.VERBOSE > 1 {
					log.Printf("unable to fetch blocks for url=%s dataset=%s error=%v", rurl, r.Dataset, r.Error)
				}
			} else {
				for _, blk := range r.MigrationBlocks {
					out = append(out, MigrationBlock{Block: blk.Block, Order: blk.Order})
				}
			}
			delete(umap, r.Dataset)
		default:
			if len(umap) == 0 {
				exit = true
			}
			time.Sleep(time.Duration(100) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	if utils.VERBOSE > 1 {
		log.Printf("GetParentDatasetBlocks yield %d", len(out))
	}

	return out, nil
}

// helper function to check if migration input is already queued
func alreadyQueued(input string) error {
	stm := getSQL("check_migration_request")
	var args []interface{}
	args = append(args, input)
	if utils.VERBOSE > 0 {
		utils.PrintSQL(stm, args, "execute")
	}
	var mid int64
	err := DB.QueryRow(stm, args...).Scan(&mid)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	if mid != 0 {
		msg := fmt.Sprintf("migration request %s is already exist in DB with id=%d", input, mid)
		return errors.New(msg)
	}
	return nil
}

// DatasetShortRecord represents short dataset record
type DatasetShortRecord struct {
	Dataset           string `json:"dataset"`
	DatasetAccessType string `json:"dataset_access_type"`
}

// helper function to check if migration input is in VALID status
func validInput(rurl, input string) error {
	arr := strings.Split(input, "#")
	dataset := arr[0]
	rurl = fmt.Sprintf("%s/datasets?dataset=%s&detail=true&dataset_access_type=*", rurl, dataset)
	data, err := getData(rurl)
	if utils.VERBOSE > 0 {
		log.Println("validInput", rurl, string(data))
	}
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to get data for %s, error %v", rurl, err)
		}
		return Error(err, HttpRequestErrorCode, "", "dbs.migrate.validInput")
	}
	var records []Dataset
	err = json.Unmarshal(data, &records)
	if err != nil {
		return Error(err, UnmarshalErrorCode, "", "dbs.migrate.validInput")
	}
	if len(records) != 1 {
		return Error(err, DatabaseErrorCode, "", "dbs.migrate.validInput")
	}
	rec := records[0]
	dtype := rec.DatasetAccessType
	if dtype == "VALID" {
		return nil
	}
	msg := fmt.Sprintf("dataset %s has status %s", dataset, dtype)
	return errors.New(msg)
}

// helper function to return string for status ID
func statusString(status int64) string {
	var s string
	if status == IN_PROGRESS {
		s = "IN_PROGRESS"
	} else if status == PENDING {
		s = "PENDING"
	} else if status == COMPLETED {
		s = "COMPLETED"
	} else if status == FAILED {
		s = "FAILED"
	} else if status == TERM_FAILED {
		s = "TERMINATED"
	} else if status == EXIST_IN_DB {
		s = "EXIST_IN_DB"
	} else if status == QUEUED {
		s = "QUEUED"
	}
	return s
}

// SubmitMigration DBS API
func (a *API) SubmitMigration() error {

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("unable to read from reader", err)
		return Error(err, ReaderErrorCode, "", "dbs.migrate.SubmitMigration")
	}
	tstamp := time.Now().Unix()
	rec := MigrationRequest{
		MIGRATION_STATUS:       QUEUED,
		CREATE_BY:              a.CreateBy,
		CREATION_DATE:          tstamp,
		LAST_MODIFIED_BY:       a.CreateBy,
		LAST_MODIFICATION_DATE: tstamp,
	}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("unable to unmarshal migration request", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.migrate.SubmitMigration")
	}
	log.Println("submit migration request ", string(data))
	// check if migration input is already queued
	input := rec.MIGRATION_INPUT
	mid := rec.MIGRATION_REQUEST_ID
	mstr := fmt.Sprintf("Migration request %s, id=%d", input, mid)
	if err := alreadyQueued(input); err != nil {
		msg := fmt.Sprintf("%s already queued error %v", mstr, err)
		if utils.VERBOSE > 1 {
			log.Println(msg)
		}
		return Error(err, MigrationErrorCode, mstr, "dbs.migrate.SubmitMigration")
	}
	// check if given input is in VALID state in DBS
	if err := validInput(rec.MIGRATION_URL, input); err != nil {
		return Error(err, MigrationErrorCode, "not allowed for migration", "dbs.migrate.SubmitMigration")
	}

	// migration output
	var reports []MigrationReport
	msg := "Migration request is started"

	// insert migration request
	tx, err := DB.Begin()
	defer tx.Rollback()
	if err != nil {
		msg = fmt.Sprintf("%s, DB connection error %v", mstr, err)
	} else {
		err = rec.Insert(tx)
		if err != nil {
			msg = fmt.Sprintf("%s, insert error %v", mstr, err)
		} else {
			// commit transaction
			err = tx.Commit()
			if err != nil {
				msg = fmt.Sprintf("%s commit transaction error %v", mstr, err)
			}
		}
	}

	// start migration request
	go StartMigrationRequest(rec)

	// create final report for our migration request
	reports = append(reports, migrationReport(rec, msg, QUEUED, nil))

	data, err = json.Marshal(reports)
	if err != nil {
		return Error(err, MarshalErrorCode, "", "dbs.migrate.SubmitMigration")
	}
	a.Writer.Write(data)
	return nil
}

// StartMigrationRequest starts asynchronously migration request process via
// goroutine with timeout context
// the code is based on the following example:
// https://medium.com/geekculture/timeout-context-in-go-e88af0abd08d
func StartMigrationRequest(rec MigrationRequest) {
	// set GlobalLog if it is set
	if GlobalLog != nil {
		log.SetOutput(GlobalLog)
	}
	// setup context with timeout
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(MigrationAsyncTimeout)*time.Second)
	defer cancel()
	ch := make(chan string, 1)
	go func(ctx context.Context, ch chan string) {
		reports, err := startMigrationRequest(rec)
		if err != nil {
			ch <- fmt.Sprintf("fail to start migration request %v, error %v", rec, err)
		} else {
			ch <- fmt.Sprintf("finished %+v with %d migration requests", rec, len(reports))
		}
	}(ctx, ch)
	select {
	case <-ctx.Done():
		msg := fmt.Sprintf("Migration request %v with context is cancelled %v", rec, ctx.Err())
		log.Println(msg)
	case response := <-ch:
		log.Println(response)
		//     case <-time.After(50 * time.Millisecond):
		//         msg := fmt.Sprintf("Migration request %v timeout", rec)
		//         log.Println(msg)
	}
}

// helper function to start migration request and return list of migration ids
//gocyclo:ignore
func startMigrationRequest(req MigrationRequest) ([]MigrationReport, error) {
	var err error
	status := int64(PENDING)
	msg := "Migration request is started"
	var reports []MigrationReport

	input := req.MIGRATION_INPUT
	mstr := fmt.Sprintf("Migration request for %+v", input)
	if utils.VERBOSE > 0 {
		log.Printf("%s %+v", mstr, req)
	}

	var dstParentBlocks, srcParentBlocks []string
	rurl := req.MIGRATION_URL
	localhost := fmt.Sprintf("%s%s", utils.Localhost, utils.BASE)
	// get parent blocks at destination DBS instance for given input
	time0 := time.Now()
	dstParentBlocks = prepareMigrationList(rurl, input)
	dstParentBlocks = utils.Set(dstParentBlocks)
	if utils.VERBOSE > 0 {
		log.Printf("Migration blocks from destination %s, total %d, elapsed time %v", rurl, len(dstParentBlocks), time.Since(time0))
		for _, b := range dstParentBlocks {
			log.Println(b)
		}
	}
	// get parent blocks at source DBS instance for given input
	//     srcParentBlocks = prepareMigrationList(localhost, input)
	time0 = time.Now()
	srcParentBlocks = prepareMigrationListAtSource(localhost, dstParentBlocks)
	srcParentBlocks = utils.Set(srcParentBlocks)
	if utils.VERBOSE > 0 {
		log.Printf("Migration blocks from source %s, total %d, elapsed time %v", localhost, len(srcParentBlocks), time.Since(time0))
		for _, b := range srcParentBlocks {
			log.Println(b)
		}
	}

	// get list of blocks required for migration
	var migBlocks []string
	for _, blk := range dstParentBlocks {
		if !utils.InList(blk, srcParentBlocks) {
			migBlocks = append(migBlocks, blk)
		}
	}

	// if input is a dataset we should find its blocks and add them for migration
	if !strings.Contains(input, "#") {
		blocks, err := GetBlocks(rurl, input)
		if err != nil {
			msg = fmt.Sprintf("unable to get blocks for dataset %s", input)
			log.Println(msg)
			return []MigrationReport{migrationReport(req, msg, status, err)},
				Error(err, DatabaseErrorCode, msg, "dbs.migrate.startMigrationRequest")
		}
		for _, blk := range blocks {
			if !utils.InList(blk, srcParentBlocks) && !utils.InList(blk, migBlocks) {
				migBlocks = append(migBlocks, blk)
			}
		}
		// add dataset itself to the list of migration
		if !utils.InList(input, migBlocks) {
			migBlocks = append(migBlocks, input)
		}
	}

	// if no migration blocks found to process return immediately
	if len(migBlocks) == 0 {
		status = int64(EXIST_IN_DB)
		req.MIGRATION_STATUS = EXIST_IN_DB
		updateMigrationStatus(req, EXIST_IN_DB)
		msg = fmt.Sprintf("%s is already fulfilled, no blocks found for migration", mstr)
		log.Println(msg)
		return []MigrationReport{migrationReport(req, msg, status, err)}, nil
	}
	if utils.VERBOSE > 0 {
		log.Printf("%s will migrate %d blocks", mstr, len(migBlocks))
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg = fmt.Sprintf("%s, unable to get DB connection", mstr)
		log.Println(msg)
		return []MigrationReport{migrationReport(req, msg, status, err)},
			Error(err, TransactionErrorCode, "", "dbs.migrate.startMigrationRequest")
	}
	defer tx.Rollback()

	// add our block input to migration blocks
	if !utils.InList(input, migBlocks) && strings.Contains(input, "#") {
		migBlocks = append(migBlocks, input)
	}

	if utils.VERBOSE > 0 {
		log.Println("final set of blocks for migrationt input", input)
		for _, blk := range migBlocks {
			log.Println("migration block", blk)
		}
	}

	// loop over migBlocks
	// and insert every chunk of blocks as MigrationBlocks objects
	var ids []int64
	for idx, blk := range migBlocks {

		// create and insert MigrationRequest object with migration blocks
		rec := req.Copy()
		rec.MIGRATION_REQUEST_ID = 0
		rec.MIGRATION_INPUT = blk
		rec.MIGRATION_STATUS = int64(PENDING)
		if utils.VERBOSE > 0 {
			log.Printf("%s insert MigrationRequest record %+v", mstr, rec)
		}
		// we skip insert for migration request input since it is inserted upstream
		if blk != input {
			err = rec.Insert(tx)
			if err != nil {
				msg = fmt.Sprintf("unable to insert MigrationRequest record %+v, error %v", rec, err)
				log.Println(msg)
				if strings.Contains(err.Error(), "unique") {
					// we inserted the same block
					continue
				}
				return []MigrationReport{migrationReport(req, msg, status, err)},
					Error(err, InsertErrorCode, "", "dbs.migrate.SubmitMigration")
			}
		}

		// get inserted migration ID
		rid, err := GetID(tx, "MIGRATION_REQUESTS", "MIGRATION_REQUEST_ID", "MIGRATION_INPUT", blk)
		if err != nil {
			msg = fmt.Sprintf("unable to get MIGRATION_REQUESTS id, error %v", err)
			if utils.VERBOSE > 1 {
				log.Println(msg)
			}
			return []MigrationReport{migrationReport(req, msg, status, err)},
				Error(err, GetIDErrorCode, "", "dbs.migrate.SubmitMigration")
		}

		// set migration record
		status := int64(PENDING)
		mrec := MigrationBlocks{
			MIGRATION_REQUEST_ID:   rid,
			MIGRATION_BLOCK_NAME:   blk,
			MIGRATION_ORDER:        int64(idx),
			MIGRATION_STATUS:       status,
			CREATE_BY:              rec.CREATE_BY,
			CREATION_DATE:          rec.CREATION_DATE,
			LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE,
			LAST_MODIFIED_BY:       rec.LAST_MODIFIED_BY}
		if utils.VERBOSE > 0 {
			log.Printf("%s insert MigrationBlocks record %+v", mstr, mrec)
		}
		err = mrec.Insert(tx)
		if err != nil {
			msg = fmt.Sprintf("%s unable to insert MigrationBlocks record %+v, error %v", mstr, mrec, err)
			if utils.VERBOSE > 0 {
				log.Println(msg)
			}
			return []MigrationReport{migrationReport(rec, msg, status, err)},
				Error(err, InsertErrorCode, "", "dbs.migrate.SubmitMigration")
		}
		reports = append(reports, migrationReport(rec, msg, status, nil))
		ids = append(ids, rid)
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		msg = fmt.Sprintf("%s unable to commit transaction error %v", mstr, err)
		log.Println(msg)
		return []MigrationReport{migrationReport(req, msg, status, err)},
			Error(err, CommitErrorCode, "", "dbs.migrate.SubmitMigration")
	}

	if utils.VERBOSE > 0 {
		log.Printf("%s finished, migration ids %v", mstr, ids)
	}

	// after we done with insertion of migration blocks
	// we update original migration request status and set it to PENDING
	updateMigrationStatus(req, PENDING)

	return reports, nil
}

// helper function to return migrationReport
func migrationReport(req MigrationRequest, report string, status int64, err error) MigrationReport {
	r := MigrationReport{
		MigrationRequest: req,
		Report:           report,
		Status:           statusString(status),
		Error:            err,
	}
	return r
}

// ProcessMigration will process given migration request
// and inject data to source DBS
// It expects that client will provide migration_request_url and migration id
//gocyclo:ignore
func (a *API) ProcessMigration() {

	var status int64
	status = PENDING // change it if we succeed at the end

	// backward compatibility with DBS migration server which uses migration_rqst_id
	if v, ok := a.Params["migration_rqst_id"]; ok {
		a.Params["migration_request_id"] = v
	}

	// obtain migration request record
	val, err := getSingleValue(a.Params, "migration_request_id")
	if err != nil {
		log.Printf("unable to get migration_request_id", err)
		return
	}
	midint, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("unable to convert mid", err)
		return
	}
	mid := int64(midint)
	log.Println("process migration request", mid)

	records, err := MigrationRequests(mid)
	if utils.VERBOSE > 0 {
		log.Println("found process migration request records")
		for _, r := range records {
			log.Printf("%+v", r)
		}
	}
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("fail to fetch migration request %d, error %v", mid, err)
		}
		return
	}
	if len(records) != 1 {
		if utils.VERBOSE > 0 {
			log.Printf("found %d requests for mid=%d, stop processing", len(records), mid)
		}
		return
	}
	mrec := records[0]

	// update migration status
	updateMigrationStatus(mrec, IN_PROGRESS)

	// find block name for our migration id
	stm := getSQL("migration_block")
	stm = CleanStatement(stm)
	var args []interface{}
	args = append(args, mid)
	if utils.VERBOSE > 0 {
		utils.PrintSQL(stm, args, "execute")
	}
	var bid, bOrder, bStatus int64
	var migInput string
	err = DB.QueryRow(stm, args...).Scan(
		&bid, &migInput, &bOrder, &bStatus,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
	// if status of migration block is completed then we update status of migration request
	if bid != 0 && bStatus == int64(COMPLETED) {
		status = COMPLETED
		updateMigrationStatus(mrec, COMPLETED)
		return
	}

	// check if our migration input is block name or dataset
	if !strings.Contains(migInput, "#") {
		// if we got dataset name we simply check its presence and update the status
		localhost := fmt.Sprintf("%s%s", utils.Localhost, utils.BASE)
		blocks, err := GetBlocks(localhost, migInput)
		if err == nil {
			for _, blk := range blocks {
				if strings.Contains(migInput, blk) {
					status = COMPLETED
					updateMigrationStatus(mrec, COMPLETED)
					return
				}
			}
		} else {
			if utils.VERBOSE > 0 {
				log.Printf("unable to get blocks from %s for migration input %s, error %v", localhost, migInput, err)
			}
		}
		status = FAILED
		updateMigrationStatus(mrec, FAILED)
		return
	}

	// we will proceed with block name as migration input
	block := migInput

	// obtain block details from destination DBS
	rurl := fmt.Sprintf("%s/blockdump?block_name=%s", mrec.MIGRATION_URL, url.QueryEscape(block))
	data, err := getData(rurl)
	if utils.VERBOSE > 1 {
		log.Println("place call", rurl)
		if utils.VERBOSE > 3 {
			log.Println("receive data", string(data))
		}
	}
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Printf("unable to query %s/blockdump, error %v", rurl, err)
		}
		return
	}
	// NOTE: /blockdump API returns BulkBlocks record used in /bulkblocks API
	//     var rec BlockDumpRecord
	var brec BulkBlocks
	err = json.Unmarshal(data, &brec)
	if err != nil {
		if utils.VERBOSE > 2 {
			log.Println("blockdump data", string(data))
		}
		log.Printf("unable to unmarshal BulkBlocks, error %v", err)
		return
	}
	cby := a.CreateBy
	if brec.Dataset.CreateBy != "" {
		cby = brec.Dataset.CreateBy
	}
	var rec Record
	err = json.Unmarshal(data, &rec)
	if err != nil {
		if utils.VERBOSE > 2 {
			log.Println("blockdump data", string(data))
		}
		log.Printf("unable to unmarshal Record, error %v", err)
		return
	}
	reader := bytes.NewReader(data)
	writer := utils.StdoutWriter("")

	// insert block dump record into source DBS
	//     err = rec.InsertBlockDump()
	api := &API{
		Params:    rec,
		Api:       "bulkblocks",
		Writer:    writer,
		Reader:    reader,
		CreateBy:  cby,
		Separator: a.Separator,
	}
	if utils.VERBOSE > 2 {
		log.Printf("Insert bulkblocks %+v, data %+v", api, string(data))
	}
	if ConcurrentBulkBlocks {
		err = api.InsertBulkBlocksConcurrently()
	} else {
		err = api.InsertBulkBlocks()
	}
	log.Printf("insert bulkblocks for mid %v error %v", mid, err)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("insert block dump record failed with", err)
		}
		status = FAILED
		updateMigrationStatus(mrec, FAILED)
	} else {
		status = COMPLETED
		updateMigrationStatus(mrec, COMPLETED)
	}
	log.Printf("updated migration request %v with status %v", mid, status)
}

// ProcessMigrationCtx will process given migration request
// and inject data to source DBS with timeout context
// It expects that client will provide migration_request_url and migration id
func (a *API) ProcessMigrationCtx(timeout int) error {

	var status int64
	var err error
	var msg string

	// setup context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	// create channel to report when operation will be completed
	ch := make(chan bool)
	defer close(ch)

	// set default status
	status = PENDING

	// backward compatibility with DBS migration server which uses migration_rqst_id
	if v, ok := a.Params["migration_rqst_id"]; ok {
		a.Params["migration_request_id"] = v
	}

	// obtain migration request record
	val, err := getSingleValue(a.Params, "migration_request_id")
	if err != nil {
		log.Printf("unable to get migration_request_id", err)
		return Error(err, ParametersErrorCode, "", "dbs.migrate.ProcessMigrationCtx")
	}
	midint, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("unable to convert mid", err)
		return Error(err, ParseErrorCode, "", "dbs.migrate.ProcessMigrationCtx")
	}
	mid := int64(midint)
	log.Println("process migration request", mid)

	records, err := MigrationRequests(mid)
	if utils.VERBOSE > 0 {
		log.Println("found process migration request records", records)
	}
	if err != nil {
		msg := fmt.Sprintf("fail to fetch migration request %d, error %v", mid, err)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(err, MigrationErrorCode, msg, "dbs.migrate.ProcessMigrationCtx")
	}
	if len(records) != 1 {
		msg := fmt.Sprintf("found %d requests for mid=%d, stop processing", len(records), mid)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(errors.New(msg), MigrationErrorCode, "", "dbs.migrate.ProcessMigrationCtx")
	}
	mrec := records[0]

	// execute slow operation in background
	go a.processMigration(ch, &status, mrec)

	// the slow operation will either finish or timeout
	select {
	case <-ctx.Done():
		msg = fmt.Sprintf("Process migration function timeout")
		err = errors.New(msg)
	case <-ch:
		msg = fmt.Sprintf("migration request completed with status %v", status)
		log.Println(msg)
	}
	reports := []MigrationReport{migrationReport(mrec, msg, status, err)}
	if a.Writer != nil {
		data, err := json.Marshal(reports)
		if err == nil {
			a.Writer.Write(data)
		} else {
			data := fmt.Sprintf(
				"fail to marshal migration record %+v, status %v error %v",
				mrec, status, err)
			a.Writer.Write([]byte(data))
		}
	}
	if err != nil {
		return Error(err, MigrationErrorCode, "", "dbs.migrate.ProcessMigrationCtx")
	}
	return nil
}

// processMigration will process given migration report
// and inject data to source DBS
func (a *API) processMigration(ch chan<- bool, status *int64, mrec MigrationRequest) {
	// report on channel that we are done with this workflow
	defer func() {
		ch <- true
	}()

	mid := mrec.MIGRATION_REQUEST_ID

	// update migration status
	updateMigrationStatus(mrec, IN_PROGRESS)

	// find block name for our migration id
	stm := getSQL("migration_block")
	stm = CleanStatement(stm)
	var args []interface{}
	args = append(args, mid)
	if utils.VERBOSE > 0 {
		utils.PrintSQL(stm, args, "execute")
	}
	var bid, bOrder, bStatus int64
	var block string
	err := DB.QueryRow(stm, args...).Scan(
		&bid, &block, &bOrder, &bStatus,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}

	// obtain block details from destination DBS
	rurl := fmt.Sprintf("%s/blockdump?block_name=%s", mrec.MIGRATION_URL, url.QueryEscape(block))
	data, err := getData(rurl)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Printf("unable to query %s/blockdump, error %v", rurl, err)
		}
	}
	// NOTE: /blockdump API returns BulkBlocks record used in /bulkblocks API
	var brec BulkBlocks
	err = json.Unmarshal(data, &brec)
	if err != nil {
		if utils.VERBOSE > 2 {
			log.Println("blockdump data", string(data))
		}
		log.Printf("unable to unmarshal BulkBlocks, error %v", err)
		return
	}
	cby := a.CreateBy
	if brec.Dataset.CreateBy != "" {
		cby = brec.Dataset.CreateBy
	}
	var rec Record
	err = json.Unmarshal(data, &rec)
	if err != nil {
		if utils.VERBOSE > 2 {
			log.Println("blockdump data", string(data))
		}
		log.Printf("unable to unmarshal Record, error %v", err)
		return
	}
	reader := bytes.NewReader(data)
	writer := utils.StdoutWriter("")

	// insert block dump record into source DBS
	//     err = rec.InsertBlockDump()
	api := &API{
		Params:    rec,
		Api:       "bulkblocks",
		Writer:    writer,
		Reader:    reader,
		CreateBy:  cby,
		Separator: a.Separator,
	}
	if utils.VERBOSE > 2 {
		log.Printf("Insert bulkblocks %+v, data %+v", api, string(data))
	}
	if ConcurrentBulkBlocks {
		err = api.InsertBulkBlocksConcurrently()
	} else {
		err = api.InsertBulkBlocks()
	}
	log.Printf("insert bulk blocks for mid %v error %v", mid, err)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("insert block dump record failed with", err)
		}
		*status = FAILED
		updateMigrationStatus(mrec, FAILED)
	} else {
		*status = COMPLETED
		updateMigrationStatus(mrec, COMPLETED)
	}
	log.Printf("updated migration request %v with status %v", mid, *status)
}

// helper function to check host of migation request
func migrationHost(mid int64) (string, error) {
	// check if migration server is empty when migration status is IN_PROGRESS
	// we use sql.NullString as migration server info may not be present in DB
	// https://medium.com/aubergine-solutions/how-i-handled-null-possible-values-from-database-rows-in-golang-521fb0ee267
	var msrv sql.NullString
	stm := getSQL("check_migration_server")
	err := DB.QueryRow(stm, mid).Scan(&msrv)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
		log.Println(msg)
		return "", Error(err, QueryErrorCode, "", "dbs.migrate.migrationHost")
	}
	migServer := msrv.String
	hostname, err := os.Hostname()
	if err != nil {
		return "", Error(err, GenericErrorCode, "", "dbs.migrate.migrationHost")
	}

	// on k8s if migServer differ from hostname we need to check if such pod exists
	out, err := exec.Command("kubectl", "get", "pods", "-n", "dbs").Output()
	// we migration server among the pods
	if err == nil && strings.Contains(string(out), migServer) {
		// compare migration server to hostname
		if migServer != "" && migServer != hostname {
			msg := fmt.Sprintf("migration request %d is already taken by %s", mid, migServer)
			log.Println(msg)
			return "", Error(ConcurrencyErr, MigrationErrorCode, msg, "dbs.migrate.migrationHost")

		}
	}
	// otherwise we do not have a pod with name of migration server
	// or we do not run migration server in multi-node environment
	return hostname, nil
}

// updateMigrationStatus updates migration status and increment retry count of
// migration record.
func updateMigrationStatus(mrec MigrationRequest, status int) error {
	log.Printf("update migration request %d to status %d", mrec.MIGRATION_REQUEST_ID, status)
	tmplData := make(Record)
	tmplData["Owner"] = DBOWNER
	stm, err := LoadTemplateSQL("update_migration_status", tmplData)
	if err != nil {
		log.Println("unable to load update_migration_status template", err)
		return Error(err, LoadErrorCode, "", "dbs.migrate.updateMigrationStatus")
	}

	stm = CleanStatement(stm)
	mid := mrec.MIGRATION_REQUEST_ID
	retryCount := mrec.RETRY_COUNT

	// get migration host or fail
	hostname, err := migrationHost(mid)
	if err != nil {
		return err
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return Error(err, TransactionErrorCode, "", "dbs.migrate.updateMigrationStatus")
	}
	defer tx.Rollback()

	// if our status is FAILED we check for retry count
	// if retry count is less then threshold we increment retry count and set status to IN PROGRESS
	// this will allow migration service to pick up failed migration request
	// otherwise we permanently terminate the migration request and set its status to TERM_FAILED
	if status == FAILED {
		if retryCount <= MigrationRetries {
			retryCount += 1
			status = IN_PROGRESS
		} else {
			status = TERM_FAILED
		}
	}
	if utils.VERBOSE > 0 {
		var args []interface{}
		args = append(args, status)
		args = append(args, retryCount)
		args = append(args, hostname)
		args = append(args, mid)
		utils.PrintSQL(stm, args, "execute update migration status query")
	}
	log.Printf("update migration request %d to status %d", mid, status)

	_, err = tx.Exec(stm, status, retryCount, hostname, mid)
	if err != nil {
		log.Printf("unable to execute %s, error %v", stm, err)
		return Error(err, UpdateErrorCode, "", "dbs.migrate.updateMigrationStatus")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return Error(err, CommitErrorCode, "", "dbs.migrate.updateMigrationStatus")
	}
	return nil
}

// MigrationRemoveRequest represents migration remove request object
type MigrationRemoveRequest struct {
	MIGRATION_REQUEST_ID int64 `json:"migration_rqst_id"`
}

// RemoveMigration DBS API
func (a *API) RemoveMigration() error {
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		msg := "unable to read data"
		log.Println(msg, err)
		return Error(err, ReaderErrorCode, "", "dbs.migrate.RemoveMigration")
	}
	rec := MigrationRemoveRequest{}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		msg := "unable to decode data"
		log.Println(msg, err)
		return Error(err, UnmarshalErrorCode, "", "dbs.migrate.RemoveMigration")
	}
	mid := rec.MIGRATION_REQUEST_ID

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := "unable to get DB transaction"
		log.Println(msg, err)
		return Error(err, TransactionErrorCode, "", "dbs.migrate.RemoveMigration")
	}
	defer tx.Rollback()

	stm := getSQL("count_migration_requests")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 0 {
		var args []interface{}
		args = append(args, mid)
		utils.PrintSQL(stm, args, "execute")
	}
	var tid float64
	err = tx.QueryRow(stm, mid).Scan(&tid)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
		log.Println(msg)
		return Error(err, QueryErrorCode, "", "dbs.migrate.RemoveMigration")
	}
	if utils.VERBOSE > 0 {
		log.Printf("found %v records to remove for request ID %d", tid, mid)
	}

	if tid > 0 {
		stm = getSQL("remove_migration_requests")
		stm = CleanStatement(stm)
		if utils.VERBOSE > 0 {
			var args []interface{}
			args = append(args, mid)
			utils.PrintSQL(stm, args, "execute")
		}
		_, err = tx.Exec(stm, mid)
		if err != nil {
			msg := fmt.Sprintf("fail to execute SQL statement '%s'", stm)
			if utils.VERBOSE > 0 {
				log.Println(msg)
			}
			return Error(err, RemoveErrorCode, "", "dbs.migrate.RemoveMigration")
		}
		err = tx.Commit()
		if err != nil {
			msg := "unable to commit transaction"
			log.Println(msg, err)
			return Error(err, CommitErrorCode, "", "dbs.migrate.RemoveMigration")
		}
		data := fmt.Sprintf("[{\"status\":\"success\",\"migration_request_id\":%d}]", mid)
		a.Writer.Write([]byte(data))
		return nil
	}
	msg := fmt.Sprintf(
		"unable to remove %v as it is either does not exists or its status is not failed", mid)
	return Error(InvalidRequestErr, InvalidRequestErrorCode, msg, "dbs.migrate.RemoveMigration")
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

	// backward compatibility with DBS migration server which uses migration_rqst_id
	val, ok := a.Params["migration_rqst_id"]
	if ok {
		a.Params["migration_request_id"] = val
	}

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
	if _, e := getSingleValue(a.Params, "migration_status"); e == nil {
		conds, args = AddParam("migration_status", "MR.MIGRATION_STATUS", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "block_name"); e == nil {
		tmpl["Blocks"] = true
		conds, args = AddParam("block_name", "MB.MIGRATION_BLOCK_NAME", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "create_by"); e == nil {
		conds, args = AddParam("create_by", "MR.CREATE_BY", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("migration_requests", tmpl)
	if err != nil {
		log.Println("unable to load migration_requests template", err)
		return Error(err, LoadErrorCode, "", "dbs.migrate.StatusMigration")
	}
	stm = WhereClause(stm, conds)
	if oldest == "true" {
		stm += "ORDER BY MR.creation_date"
	}

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.migrate.StatusMigration")
	}
	return nil
}

// TotalMigration DBS API
func (a *API) TotalMigration() error {
	var args []interface{}
	// get SQL statement from static area
	stm := getSQL("migration_total_count")

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.migrate.TotalMigration")
	}
	return nil
}

// CancelMigration clean-ups migration requests in DB
func (a *API) CancelMigration() error {

	// our API expect JSON payload with MigrationRemoveRequest structure
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.migrate.CancelMigration")
	}
	var r MigrationRemoveRequest
	err = json.Unmarshal(data, &r)
	if err != nil {
		log.Println("untable to unmarshal input data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.migrate.CancelMigration")
	}
	mid := r.MIGRATION_REQUEST_ID

	log.Println("process migration request", mid)

	records, err := MigrationRequests(mid)
	if utils.VERBOSE > 0 {
		log.Println("found process migration request records")
		for _, r := range records {
			log.Printf("%+v", r)
		}
	}
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("fail to fetch migration request %d, error %v", mid, err)
		}
		return Error(err, MigrationErrorCode, "", "dbs.migrate.CancelMigration")
	}
	if len(records) != 1 {
		if utils.VERBOSE > 0 {
			log.Printf("found %d requests for mid=%d, stop processing", len(records), mid)
		}
		return Error(err, MigrationErrorCode, "", "dbs.migrate.CancelMigration")
	}
	mrec := records[0]
	log.Printf("CancelMigration request %+v, status %v (TERM_FAILED)", mrec, TERM_FAILED)
	updateMigrationStatus(mrec, TERM_FAILED)
	return nil
}

// CleanupMigrationRequests clean-ups migration requests in DB
func (a *API) CleanupMigrationRequests(offset int64) error {
	tmplData := make(Record)
	tmplData["Owner"] = DBOWNER
	tmplData["Value"] = time.Now().Unix() - offset
	tmplData["FailDate"] = time.Now().Unix() - 2*7*60*60 // 2 weeks
	stm, err := LoadTemplateSQL("cleanup_migration_requests", tmplData)
	if err != nil {
		log.Println("unable to load cleanup_migration_requests template", err)
		return Error(err, LoadErrorCode, "", "dbs.migrate.CleanupMigrationRequests")
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return Error(err, TransactionErrorCode, "", "dbs.migrate.CleanupMigrationRequests")
	}
	defer tx.Rollback()
	stm = CleanStatement(stm)
	if utils.VERBOSE > 0 {
		var args []interface{}
		utils.PrintSQL(stm, args, "execute")
	}

	_, err = tx.Exec(stm)
	if err != nil {
		log.Printf("unable to execute %s, error %v", stm, err)
		return Error(err, RemoveErrorCode, "", "dbs.migrate.CleanupMigrationRequests")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return Error(err, CommitErrorCode, "", "dbs.migrate.CleanupMigrationRequests")
	}
	return nil
}
