package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/utils"
	_ "github.com/mattn/go-sqlite3"
)

// TestBulkBlocks API
func TestBulkBlocks(t *testing.T) {

	// initialize DB for testing
	dburi := os.Getenv("DBS_DB_FILE")
	if dburi == "" {
		log.Fatal("DBS_DB_FILE not defined")
	}
	db := initDB(false, dburi)
	defer db.Close()

	//     var api dbs.API
	var err error
	var data []byte
	//     var reader *bytes.Reader
	ts := time.Now().Unix()
	createBy := "tester"
	w := utils.StdoutWriter("")
	api := dbs.API{
		Writer:   w,
		CreateBy: createBy,
	}

	// to insert bulkblock.json we need to have certain data in place
	// below we list attributes we use in bulkblock.json
	tier := dbs.DataTiers{DATA_TIER_NAME: "GEN-SIM-RAW", CREATION_DATE: ts, CREATE_BY: createBy}
	data, _ = json.Marshal(tier)
	api.Reader = bytes.NewReader(data)
	err = api.InsertDataTiers()
	if err != nil {
		t.Fatalf("Fail to insert data tier %v\n", err)
	}
	physGrp := dbs.PhysicsGroups{PHYSICS_GROUP_NAME: "Tracker"}
	data, _ = json.Marshal(physGrp)
	api.Reader = bytes.NewReader(data)
	err = api.InsertPhysicsGroups()
	if err != nil {
		t.Fatalf("Fail to insert physics group %v\n", err)
	}
	dacc := dbs.DatasetAccessTypes{DATASET_ACCESS_TYPE: "PRODUCTION"}
	data, _ = json.Marshal(dacc)
	api.Reader = bytes.NewReader(data)
	err = api.InsertDatasetAccessTypes()
	if err != nil {
		t.Fatalf("Fail to insert dataset access type %v\n", err)
	}
	procDS := dbs.ProcessedDatasets{PROCESSED_DS_NAME: "Summer2011-pstr-v10"}
	data, _ = json.Marshal(procDS)
	api.Reader = bytes.NewReader(data)
	err = api.InsertProcessedDatasets()
	if err != nil {
		t.Fatalf("Fail to insert processed datasets%v\n", err)
	}

	primds := dbs.PrimaryDatasetRecord{PRIMARY_DS_NAME: "unittest_web_primary_ds_name_14144", CREATION_DATE: ts, CREATE_BY: createBy, PRIMARY_DS_TYPE: "test"}
	data, _ = json.Marshal(primds)
	api.Reader = bytes.NewReader(data)
	err = api.InsertPrimaryDatasets()
	if err != nil {
		t.Fatalf("Fail to insert primary dataset %v\n", err)
	}

	// we insert parent files via transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Fail to get db transaction %v\n", err)
	}
	defer tx.Rollback()
	isValid := int64(1)
	datasetID := int64(1)
	blockID := int64(1)
	fileTypeID := int64(1)
	checkSum := "123"
	fileSize := int64(100)
	eventCount := int64(100)
	adler32 := "adler"

	dataset := dbs.Datasets{
		DATASET:                "/unittest_web_primary_ds_name_14144/Summer2011-pstr-v10/GEN-SIM-RAW",
		PRIMARY_DS_ID:          1,
		PROCESSED_DS_ID:        1,
		DATA_TIER_ID:           1,
		DATASET_ACCESS_TYPE_ID: 1,
		ACQUISITION_ERA_ID:     1,
		PROCESSING_ERA_ID:      1,
		PHYSICS_GROUP_ID:       1,
		PREP_ID:                "test-prep_id",
		CREATION_DATE:          ts,
		CREATE_BY:              createBy,
		LAST_MODIFICATION_DATE: ts,
		LAST_MODIFIED_BY:       createBy,
	}

	err = dataset.Insert(tx)
	if err != nil {
		t.Fatalf("Fail to insert dataset %v\n", err)
	}

	files := dbs.Files{
		LOGICAL_FILE_NAME:      "/store/data/a/b/A/a/1/parent/abcd3.root",
		CREATION_DATE:          ts,
		CREATE_BY:              createBy,
		LAST_MODIFICATION_DATE: ts,
		LAST_MODIFIED_BY:       createBy,
		IS_FILE_VALID:          isValid,
		DATASET_ID:             datasetID,
		BLOCK_ID:               blockID,
		FILE_TYPE_ID:           fileTypeID,
		CHECK_SUM:              checkSum,
		FILE_SIZE:              fileSize,
		EVENT_COUNT:            eventCount,
		ADLER32:                adler32,
	}
	err = files.Insert(tx)
	if err != nil {
		t.Fatalf("Fail to insert files %v\n", err)
	}
	files = dbs.Files{
		LOGICAL_FILE_NAME:      "/store/data/a/b/A/a/1/parent/abcd2.root",
		CREATION_DATE:          ts,
		CREATE_BY:              createBy,
		LAST_MODIFICATION_DATE: ts,
		LAST_MODIFIED_BY:       createBy,
		IS_FILE_VALID:          isValid,
		DATASET_ID:             datasetID,
		BLOCK_ID:               blockID,
		FILE_TYPE_ID:           fileTypeID,
		CHECK_SUM:              checkSum,
		FILE_SIZE:              fileSize,
		EVENT_COUNT:            eventCount,
		ADLER32:                adler32,
	}
	err = files.Insert(tx)
	if err != nil {
		t.Fatalf("Fail to insert files %v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Fail to commit %v\n", err)
	}

	// read bulkblocks.json from test area and process it
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Fail to get current directory %v\n", err)
	}
	fname := fmt.Sprintf("%s/data/bulkblocks0.json", dir)
	data, err = ioutil.ReadFile(fname)
	if err != nil {
		t.Fatalf("Fail to read file %s, error %v\n", fname, err)
	}
	api.Reader = bytes.NewReader(data)
	err = api.InsertBulkBlocks()
	if err != nil {
		t.Fatalf("Fail to process bulkblocks data %v\n", err)
	}
}
