package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
)

// TestBulkBlocks API
func TestBulkBlocks(t *testing.T) {

	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	var api dbs.API
	var err error
	var data []byte
	var reader *bytes.Reader
	ts := time.Now().Unix()
	createBy := "tester"

	// to insert bulkblock.json we need to have certain data in place
	// below we list attributes we use in bulkblock.json
	tier := dbs.DataTiers{DATA_TIER_NAME: "GEN-SIM-RAW", CREATION_DATE: ts, CREATE_BY: createBy}
	data, _ = json.Marshal(tier)
	reader = bytes.NewReader(data)
	_, err = api.InsertDataTiers(reader, createBy)
	if err != nil {
		t.Errorf("Fail to insert data tier %v\n", err)
	}
	physGrp := dbs.PhysicsGroups{PHYSICS_GROUP_NAME: "Tracker"}
	data, _ = json.Marshal(physGrp)
	reader = bytes.NewReader(data)
	_, err = api.InsertPhysicsGroups(reader, createBy)
	if err != nil {
		t.Errorf("Fail to insert physics group %v\n", err)
	}
	dacc := dbs.DatasetAccessTypes{DATASET_ACCESS_TYPE: "PRODUCTION"}
	data, _ = json.Marshal(dacc)
	reader = bytes.NewReader(data)
	_, err = api.InsertDatasetAccessTypes(reader, createBy)
	if err != nil {
		t.Errorf("Fail to insert dataset access type %v\n", err)
	}
	procDS := dbs.ProcessedDatasets{PROCESSED_DS_NAME: "Summer2011-pstr-v10"}
	data, _ = json.Marshal(procDS)
	reader = bytes.NewReader(data)
	_, err = api.InsertProcessedDatasets(reader, createBy)
	if err != nil {
		t.Errorf("Fail to insert dataset access type %v\n", err)
	}

	// we insert parent files via transaction
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("Fail to get db transaction %v\n", err)
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
	files := dbs.Files{LOGICAL_FILE_NAME: "/store/data/a/b/A/a/1/parent/abcd3.root", CREATION_DATE: ts, CREATE_BY: createBy, LAST_MODIFICATION_DATE: ts, LAST_MODIFIED_BY: createBy, IS_FILE_VALID: isValid, DATASET_ID: datasetID, BLOCK_ID: blockID, FILE_TYPE_ID: fileTypeID, CHECK_SUM: checkSum, FILE_SIZE: fileSize, EVENT_COUNT: eventCount, ADLER32: adler32}
	err = files.Insert(tx)
	if err != nil {
		t.Errorf("Fail to insert files %v\n", err)
	}
	files = dbs.Files{LOGICAL_FILE_NAME: "/store/data/a/b/A/a/1/parent/abcd2.root", CREATION_DATE: ts, CREATE_BY: createBy, LAST_MODIFICATION_DATE: ts, LAST_MODIFIED_BY: createBy, IS_FILE_VALID: isValid, DATASET_ID: datasetID, BLOCK_ID: blockID, FILE_TYPE_ID: fileTypeID, CHECK_SUM: checkSum, FILE_SIZE: fileSize, EVENT_COUNT: eventCount, ADLER32: adler32}
	err = files.Insert(tx)
	if err != nil {
		t.Errorf("Fail to insert files %v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Errorf("Fail to commit %v\n", err)
	}

	// read bulkblocks.json from test area and process it
	dir, err := os.Getwd()
	if err != nil {
		t.Errorf("Fail to get current directory %v\n", err)
	}
	fname := fmt.Sprintf("%s/bulkblocks.json", dir)
	data, err = ioutil.ReadFile(fname)
	if err != nil {
		t.Errorf("Fail to read file %s, error %v\n", fname, err)
	}
	reader = bytes.NewReader(data)
	_, err = api.InsertBulkBlocks(reader, createBy)
	if err != nil {
		t.Errorf("Fail to process bulkblocks data %v\n", err)
	}
}
