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
//
// To properly run the test, the six servers must be started using ./bin/start_test_migration

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

// remove migraiton request
type removeMigrationRequest struct {
	MigrationRequestId int64  `json:"migration_rqst_id"`
	CreateBy           string `json:"create_by"`
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

	bulk := BulkBlocksData.ConcurrentParentData

	headers := http.Header{
		"Accept":       []string{"application/json"},
		"Content-Type": []string{"application/json"},
	}

	t.Run("Inserting a bulkblock into dbs-one", func(t *testing.T) {
		data, err := json.Marshal(bulk)
		if err != nil {
			t.Fatal(err.Error())
		}
		reader := bytes.NewReader(data)

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
	})

	t.Run("Verify bulkblock insert into dbs-one", func(t *testing.T) {
		// verify insert
		verifyBulkBlocksInsert(t, srv1, base1, false)
	})

	var migrationID int64

	t.Run("Create a migration request", func(t *testing.T) {
		mr := migrationRequest{
			MigrationURL:   fmt.Sprintf("%s/%s", srv1, base1),
			MigrationInput: bulk.Block.BlockName,
		}
		data, err := json.Marshal(mr)
		if err != nil {
			t.Fatal(err.Error())
		}
		reader := bytes.NewReader(data)
		path := fmt.Sprintf("%s/submit", base5)
		req := newreq(t, "POST", srv5, path, reader, nil, headers)
		r, err := http.DefaultClient.Do(req)
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

		migrationID = d[0].MigrationRequest.MIGRATION_REQUEST_ID
	})

	if migrationID == 0 {
		t.Fatalf("No migrationID")
	}

	t.Run("Process migration request", func(t *testing.T) {
		pmr := processMigrationRequest{
			MigrationRequestId: migrationID,
		}
		data, err := json.Marshal(pmr)
		if err != nil {
			t.Fatal(err.Error())
		}
		reader := bytes.NewReader(data)

		path := fmt.Sprintf("%s/process", base5)
		req := newreq(t, "POST", srv5, path, reader, nil, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Process failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var d []dbs.MigrationReport

		err = json.NewDecoder(r.Body).Decode(&d)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}
		fmt.Printf("Process response: %+v\n", d)
	})

	t.Run("Get status of the migration", func(t *testing.T) {
		path := fmt.Sprintf("%s/status", base5)
		req := newreq(t, "GET", srv5, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Status failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var d []dbs.MigrationReport

		err = json.NewDecoder(r.Body).Decode(&d)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}
		fmt.Printf("Status response: %+v\n", d)
	})

	t.Run("Get status of the single migration", func(t *testing.T) {
		// status of single migration
		params := url.Values{
			"migration_request_id": []string{fmt.Sprintf("%v", migrationID)},
		}
		path := fmt.Sprintf("%s/status", base5)
		req := newreq(t, "GET", srv5, path, nil, params, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Status failed! Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var d []dbs.MigrationReport

		err = json.NewDecoder(r.Body).Decode(&d)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}
		fmt.Printf("Status response: %+v\n", d)
	})

	t.Run("Get total number of migration requests", func(t *testing.T) {
		// total number of migration requests
		path := fmt.Sprintf("%s/total", base5)
		req := newreq(t, "GET", srv5, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
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
	})

	t.Run("Verify bulkblock insert into dbs-two", func(t *testing.T) {
		// verify insert
		verifyBulkBlocksInsert(t, srv3, base3, true)
	})
}

// verify bulkblocks insert
func verifyBulkBlocksInsert(t *testing.T, server string, base string, isSecond bool) {
	bulk := BulkBlocksData.ConcurrentParentData

	headers := http.Header{
		"Accept":       []string{"application/json"},
		"Content-Type": []string{"application/json"},
	}

	t.Run("check primary dataset types", func(t *testing.T) {
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
	})

	t.Run("check primary datasets", func(t *testing.T) {
		path := fmt.Sprintf("%s/primarydatasets", base)
		req := newreq(t, "GET", server, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
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
	})

	t.Run("check output configs", func(t *testing.T) {
		path := fmt.Sprintf("%s/outputconfigs", base)
		req := newreq(t, "GET", server, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
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
			CREATE_BY:           "DBS-workflow",
			CREATION_DATE:       0,
		}

		verifyResponse(t, oc, []Response{ocResp})
	})

	t.Run("check acquisition eras", func(t *testing.T) {
		path := fmt.Sprintf("%s/acquisitioneras", base)
		req := newreq(t, "GET", server, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var ae []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&ae)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}

		aeResp := dbs.AcquisitionEra{
			AcquisitionEraName: bulk.AcquisitionEra.AcquisitionEraName,
			StartDate:          0,
			EndDate:            0,
			CreationDate:       0,
			CreateBy:           "DBS-workflow",
			Description:        bulk.AcquisitionEra.Description,
		}

		verifyResponse(t, ae, []Response{aeResp})
	})

	t.Run("check processing eras", func(t *testing.T) {
		path := fmt.Sprintf("%s/processingeras", base)
		req := newreq(t, "GET", server, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var pe []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&pe)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}

		peResp := bulk.ProcessingEra

		verifyResponse(t, pe, []Response{peResp})
	})

	t.Run("check datatiers", func(t *testing.T) {
		path := fmt.Sprintf("%s/datatiers", base)
		req := newreq(t, "GET", server, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var dt []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&dt)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}

		dtResp := dbs.DataTiers{
			DATA_TIER_ID:   1,
			DATA_TIER_NAME: bulk.Dataset.DataTierName,
			CREATE_BY:      "DBS-workflow",
			CREATION_DATE:  0,
		}

		if isSecond {
			dtResp.CREATE_BY = "WMAgent"
		}

		verifyResponse(t, dt, []Response{dtResp})
	})

	t.Run("check dataset access types", func(t *testing.T) {
		path := fmt.Sprintf("%s/datasetaccesstypes", base)
		req := newreq(t, "GET", server, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var dsat []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&dsat)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}

		dsatresp := datasetAccessTypeResponse{
			DATASET_ACCESS_TYPE: bulk.Dataset.DatasetAccessType,
		}

		verifyResponse(t, dsat, []Response{dsatresp})
	})

	t.Run("check physics groups", func(t *testing.T) {
		path := fmt.Sprintf("%s/physicsgroups", base)
		req := newreq(t, "GET", server, path, nil, nil, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var pg []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&pg)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}

		pgResp := physicsGroupsResponse{
			PHYSICS_GROUP_NAME: bulk.Dataset.PhysicsGroupName,
		}

		verifyResponse(t, pg, []Response{pgResp})
	})

	t.Run("check datasets", func(t *testing.T) {
		params := url.Values{
			"dataset_access_type": []string{bulk.Dataset.DatasetAccessType},
		}
		path := fmt.Sprintf("%s/datasets", base)
		req := newreq(t, "GET", server, path, nil, params, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var ds []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&ds)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}
		t.Logf("%s", req.RequestURI)

		dsResp := createDSResponse(bulk.Dataset.Dataset)

		verifyResponse(t, ds, []Response{dsResp})
	})

	t.Run("check blocks", func(t *testing.T) {
		params := url.Values{
			"dataset": []string{bulk.Dataset.Dataset},
		}
		path := fmt.Sprintf("%s/blocks", base)
		req := newreq(t, "GET", server, path, nil, params, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var b []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&b)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}
		t.Logf("%s", req.RequestURI)

		bResp := blockResponse{
			BLOCK_NAME: bulk.Block.BlockName,
		}

		verifyResponse(t, b, []Response{bResp})
	})

	t.Run("check files", func(t *testing.T) {
		params := url.Values{
			"dataset": []string{bulk.Dataset.Dataset},
		}
		path := fmt.Sprintf("%s/files", base)
		req := newreq(t, "GET", server, path, nil, params, headers)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Different HTTP Status: Expected 200, Received %v", r.StatusCode)
		}

		var f []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&f)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}
		t.Logf("%s", req.RequestURI)

		var fResp []Response
		for _, fl := range bulk.Files {
			fileResp := fileResponse{
				LOGICAL_FILE_NAME: fl.LogicalFileName,
			}
			fResp = append(fResp, fileResp)
		}

		verifyResponse(t, f, fResp)
	})

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
