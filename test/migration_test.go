package main

// Migration Tests
// This file contains code necessary to run DBS migration workflows
//   1. DBSReader server from which we will read the data for migration process
//   2. DBSWriter server associated with DBSReader which we will use to write the data
//   3. DBSReader server from which we will verify the data from migration process
//   4. DBSWriter server which we will use to write the data from migration process
//   5. DBSMigrate server which we will use to post migration requests
//   6. DBSMigration server which will process our migration requests
// In addition, we have two databases
//   1. DBS_DB_FILE_1 represents DBS db which we will use for migration process
//      the DBSReader (1)/DBSWriter (2) will be associated with it
//   2. DBS_DB_FILE_2 represents DBS db where data will be migrated
//      the DBSReader (3)/DBSWriter (4) will be associated with it

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	_ "github.com/mattn/go-sqlite3"
)

// migration document
type migrationRequest struct {
	MigrationURL   string `json:"migration_url"`
	MigrationInput string `json:"migration_input"`
}

// process request request
type processMigrationRequest struct {
	MigrationRequestId int64 `json:"migration_rqst_id"`
}

// total number response
type migrationCountResponse struct {
	Count int64 `json:"count"`
}

// TestMigration tests DBS Migration process
func TestIntMigration(t *testing.T) {
	// start DBSReader server from which we will read the data for migration process
	base1 := "dbs-one-reader"
	srv1 := "http://localhost:8989"
	checkServer(t, srv1, base1)

	// start DBSWriter server to which we will write the data
	base2 := "dbs-one-writer"
	srv2 := "http://localhost:8990"
	checkServer(t, srv2, base2)

	// start DBSReader server from which we will read the data after migration process
	base3 := "dbs-two-reader"
	srv3 := "http://localhost:8991"
	checkServer(t, srv3, base3)

	// start DBSWriter server to which we will write the data during migration process
	base4 := "dbs-two-writer"
	srv4 := "http://localhost:8992"
	checkServer(t, srv4, base4)

	// start DBSMigrate server to which we will post migration requests
	base5 := "dbs-migrate"
	srv5 := "http://localhost:8993"
	checkServer(t, srv5, base5)

	// start DBSMigration server which will process migration requests
	base6 := "dbs-migration"
	srv6 := "http://localhost:8994"
	checkServer(t, srv6, base6)

	// check for bulkblock data file
	bulkblocksPath := os.Getenv("BULKBLOCKS_DATA_FILE")
	if bulkblocksPath == "" {
		log.Fatal("BULKBLOCKS_DATA_FILE not defined")
	}

	// load bulkblocks data
	if _, err := os.Stat(bulkblocksPath); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Generating bulkblocks data")
		generateBulkBlocksData(t, bulkblocksPath)
	}
	err := readJsonFile(t, bulkblocksPath, &BulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	// insert a bulkblock
	bulk := BulkBlocksData.ConcurrentParentData
	data, err := json.Marshal(bulk)
	if err != nil {
		t.Fatal(err.Error())
	}
	reader := bytes.NewReader(data)

	headers := http.Header{
		"Accept":       []string{"application/json"},
		"Content-Type": []string{"application/json"},
	}
	path := fmt.Sprintf("%s/bulkblocks", base2)
	req := newreq(t, "POST", srv2, path, reader, nil, headers)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Bulkblocks insert failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	verifyBulkBlocksInsert(t, srv1, base1)

	// create migration request
	mr := migrationRequest{
		MigrationURL:   fmt.Sprintf("%s/%s", srv1, base1),
		MigrationInput: bulk.Block.BlockName,
	}
	data, err = json.Marshal(mr)
	if err != nil {
		t.Fatal(err.Error())
	}
	reader = bytes.NewReader(data)
	path = fmt.Sprintf("%s/submit", base5)
	req = newreq(t, "POST", srv5, path, reader, nil, headers)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Migration request failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	var d []dbs.MigrationReport
	err = json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}
	fmt.Printf("Submit Response: %v\n", d)

	migrationID := d[0].MigrationRequest.MIGRATION_REQUEST_ID

	// process migration request
	pmr := processMigrationRequest{
		MigrationRequestId: migrationID,
	}
	data, err = json.Marshal(pmr)
	if err != nil {
		t.Fatal(err.Error())
	}
	reader = bytes.NewReader(data)

	path = fmt.Sprintf("%s/process", base5)
	req = newreq(t, "POST", srv5, path, reader, nil, headers)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Process failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	err = json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}
	fmt.Printf("Process response: %+v\n", d)

	// status of migration
	path = fmt.Sprintf("%s/status", base5)
	req = newreq(t, "GET", srv5, path, nil, nil, headers)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Status failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	err = json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}
	fmt.Printf("Status response: %+v\n", d)

	// status of single migration
	params := url.Values{
		"migration_request_id": []string{fmt.Sprintf("%v", migrationID)},
	}
	path = fmt.Sprintf("%s/status", base5)
	req = newreq(t, "GET", srv5, path, nil, params, headers)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Status failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	err = json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}
	fmt.Printf("Status response: %+v\n", d)

	// total number of migration requests
	path = fmt.Sprintf("%s/total", base5)
	req = newreq(t, "GET", srv5, path, nil, nil, headers)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Status failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	var c []migrationCountResponse

	err = json.NewDecoder(r.Body).Decode(&c)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}
	fmt.Printf("Count response: %+v\n", c)

	if c[0].Count != 1 {
		t.Fatalf("Incorrect count")
	}

}

// verify bulkblocks insert
func verifyBulkBlocksInsert(t *testing.T, server string, base string) {
	bulk := BulkBlocksData.ConcurrentParentData

	headers := http.Header{
		"Accept":       []string{"application/json"},
		"Content-Type": []string{"application/json"},
	}

	// check primary dataset types
	path := fmt.Sprintf("%s/primarydstypes", base)
	req := newreq(t, "GET", server, path, nil, nil, headers)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	var pdst []dbs.Record
	err = json.NewDecoder(r.Body).Decode(&pdst)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}

	output := primaryDSTypesResponse{
		PRIMARY_DS_TYPE_ID: 1,
		DATA_TYPE:          bulk.PrimaryDataset.PrimaryDSType,
	}

	verifyResponse(t, pdst, []Response{output})

	// check primary datasets
	path = fmt.Sprintf("%s/primarydatasets", base)
	req = newreq(t, "GET", server, path, nil, nil, headers)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	var pds []dbs.Record
	err = json.NewDecoder(r.Body).Decode(&pds)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}

	pdsResp := dbs.PrimaryDataset{
		PrimaryDSId:   1,
		PrimaryDSType: bulk.PrimaryDataset.PrimaryDSType,
		PrimaryDSName: bulk.PrimaryDataset.PrimaryDSName,
		CreateBy:      bulk.Dataset.CreateBy,
		CreationDate:  0,
	}

	verifyResponse(t, pds, []Response{pdsResp})

	// check output configs
	path = fmt.Sprintf("%s/outputconfigs", base)
	req = newreq(t, "GET", server, path, nil, nil, headers)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}

	var oc []dbs.Record
	err = json.NewDecoder(r.Body).Decode(&oc)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}

	ocResp := outputConfigResponse{
		APP_NAME:            bulk.DatasetConfigList[0].AppName,
		RELEASE_VERSION:     bulk.DatasetConfigList[0].ReleaseVersion,
		PSET_HASH:           bulk.DatasetConfigList[0].PsetHash,
		GLOBAL_TAG:          bulk.DatasetConfigList[0].GlobalTag,
		OUTPUT_MODULE_LABEL: bulk.DatasetConfigList[0].OutputModuleLabel,
		CREATE_BY:           bulk.DatasetConfigList[0].CreateBy,
		CREATION_DATE:       0,
	}

	verifyResponse(t, oc, []Response{ocResp})
}

// helper function to check given server by accessing its apis end-point
func checkServer(t *testing.T, hostname, base string) {
	endpoint := fmt.Sprintf("%s/apis", base)
	headers := http.Header{
		"Accept":       []string{"application/json"},
		"Content-Type": []string{"application/json"},
	}
	r, err := http.DefaultClient.Do(newreq(t, "GET", hostname, endpoint, nil, nil, headers))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()

	if r.StatusCode != 200 {
		t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
	}
	/*
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		log.Printf("server %s APIs: %+v", hostname, string(data))
	*/
}
