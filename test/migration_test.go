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
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

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

// TestMigration tests DBS Migration for a single block
func TestIntMigration(t *testing.T) {

	t.Cleanup(func() {
		exec.Command("pkill", "dbs2go").Output()
	})

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

		fmt.Println(r.Body)

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

	var c []migrationCountResponse
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

		err = json.NewDecoder(r.Body).Decode(&c)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}
		fmt.Printf("Count response: %+v\n", c)

		if c[0].Count != 1 {
			t.Fatalf("Incorrect count")
		}
	})

	t.Run("Wait for migration requests to be completed", func(t *testing.T) {
		// get current time for timeout
		now := time.Now()
		migTimeout := 3 * time.Minute

		var requestStatus [10]int
		requestStatus[0] = len(c)
		for requestStatus[0] != 0 || requestStatus[1] != 0 || requestStatus[5] != 0 || time.Since(now) > migTimeout {
			// sleep in order to prevent sqlite database locking
			time.Sleep(10 * time.Second)

			var migrationStatus []MigrationStatus

			requestStatus = [10]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

			path := fmt.Sprintf("%s/status", base5)
			req := newreq(t, "GET", srv5, path, nil, nil, headers)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("HTTP Request Error: %v\n", err)
			}
			defer resp.Body.Close()

			err = json.NewDecoder(resp.Body).Decode(&migrationStatus)
			if err != nil {
				t.Fatalf("Failed to decode body, %v", err)
			}

			for _, m := range migrationStatus {
				requestStatus[m.MigrationStatus] += 1
			}
			fmt.Printf("Requests: %d, PENDING: %d, IN_PROGRESS: %d, COMPLETED: %d, FAILED: %d, EXIST_IN_DB: %d, QUEUED: %d, TERM_FAILED: %d\n",
				len(migrationStatus),
				requestStatus[dbs.PENDING],
				requestStatus[dbs.IN_PROGRESS],
				requestStatus[dbs.COMPLETED],
				requestStatus[dbs.FAILED],
				requestStatus[dbs.EXIST_IN_DB],
				requestStatus[dbs.QUEUED],
				requestStatus[dbs.TERM_FAILED])
		}

		if requestStatus[9] > 0 {
			t.Fatalf("%d requests failed\n", requestStatus[9])
		}

		if time.Since(now) > migTimeout {
			t.Fatalf("Migration request timeout")
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

// TestLocalMigrationRequests test DBS Migration request that has parent blocks
func TestMigrationRequests(t *testing.T) {
	var GrandparentBulkBlocksData dbs.BulkBlocks
	var ParentBulkBlocksData dbs.BulkBlocks
	var BulkBlocksData dbs.BulkBlocks

	bulkblocksPath := os.Getenv("MIGRATION_REQUESTS_PATH")
	if bulkblocksPath == "" {
		log.Fatal("MIGRATION_REQUESTS_PATH not defined")
	}

	// load grandparent data
	err := readJsonFile(t, fmt.Sprintf("%s/genericttbar_grandparent_block.json", bulkblocksPath), &GrandparentBulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	// load parent data
	err = readJsonFile(t, fmt.Sprintf("%s/genericttbar_parent_block.json", bulkblocksPath), &ParentBulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	// load block data
	err = readJsonFile(t, fmt.Sprintf("%s/genericttbar_block.json", bulkblocksPath), &BulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Cleanup(func() {
		exec.Command("pkill", "dbs2go").Output()
	})

	t.Run("Insert initial data", func(t *testing.T) {

		t.Run("Grandparent blocks", func(t *testing.T) {
			data, err := json.Marshal(GrandparentBulkBlocksData)
			if err != nil {
				t.Fatal(err.Error())
			}
			reader := bytes.NewReader(data)

			resp, err := http.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
			if err != nil {
				t.Error(err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Bad HTTP Response: %d Expected, %d Received\n", http.StatusOK, resp.StatusCode)
			}
		})

		t.Run("Parent blocks", func(t *testing.T) {
			data, err := json.Marshal(ParentBulkBlocksData)
			if err != nil {
				t.Fatal(err.Error())
			}
			reader := bytes.NewReader(data)

			resp, err := http.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
			if err != nil {
				t.Error(err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Bad HTTP Response: %d Expected, %d Received\n", http.StatusOK, resp.StatusCode)
			}
		})

		t.Run("Blocks", func(t *testing.T) {
			data, err := json.Marshal(BulkBlocksData)
			if err != nil {
				t.Fatal(err.Error())
			}
			reader := bytes.NewReader(data)

			resp, err := http.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
			if err != nil {
				t.Error(err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Bad HTTP Response: %d Expected, %d Received\n", http.StatusOK, resp.StatusCode)
			}
		})
	})

	// delay for debugging
	// time.Sleep(1 * time.Minute)

	t.Run("Test migration", func(t *testing.T) {
		var d []dbs.MigrationReport
		t.Run("Submit Migration Request", func(t *testing.T) {

			// ds := GrandparentBulkBlocksData.Dataset.Dataset
			// ds := ParentBulkBlocksData.Dataset.Dataset
			blk := BulkBlocksData.Block.BlockName

			migReq := MigrationRequest{
				MigrationURL:   "http://localhost:8989/dbs-one-reader",
				MigrationInput: blk,
			}

			body, err := json.Marshal(migReq)
			if err != nil {
				t.Fatalf("Failed to marshal json")
			}
			resp, err := http.Post("http://localhost:8993/dbs-migrate/submit", "application/json", bytes.NewBuffer(body))
			if err != nil {
				t.Fatalf("HTTP Request Error: %v\n", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Logf("Response %v", resp)
				t.Fatalf("Not Status OK: %v", resp.StatusCode)
			}

			err = json.NewDecoder(resp.Body).Decode(&d)
			if err != nil {
				t.Fatalf("Failed to decode body, %v", err)
			}

			t.Logf("Migration Report: %v\n", d)
		})

		for _, migreq := range d {
			// var failedMigrations int

			mid := migreq.MigrationRequest.MIGRATION_REQUEST_ID
			desc := fmt.Sprintf("Process migration request, id = %d", mid)
			t.Run(desc, func(t *testing.T) {
				req := processMigrationRequest{
					MigrationRequestId: mid,
				}

				body, err := json.Marshal(req)
				if err != nil {
					t.Fatalf("Failed to marshal json")
				}
				uri := fmt.Sprintf("http://localhost:8993/dbs-migrate/process?migration_request_id=%d", mid)
				resp, err := http.Post(uri, "application/json", bytes.NewBuffer(body))
				if err != nil {
					t.Fatalf("HTTP Request Error: %v\n", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					t.Logf("Response %v", resp)
					t.Fatalf("Not Status OK: %v", resp.StatusCode)
				}
			})
		}

		// get current time for timeout
		now := time.Now()
		migTimeout := 3 * time.Minute

		t.Run("Check Migration Request status", func(t *testing.T) {
			var requestStatus [10]int
			requestStatus[0] = len(d)
			for requestStatus[0] != 0 || requestStatus[1] != 0 || requestStatus[5] != 0 || time.Since(now) > migTimeout {
				// sleep in order to prevent sqlite database locking
				time.Sleep(10 * time.Second)

				var migrationStatus []MigrationStatus

				requestStatus = [10]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

				resp, err := http.Get("http://localhost:8993/dbs-migrate/status")
				if err != nil {
					t.Fatalf("HTTP Request Error: %v\n", err)
				}
				defer resp.Body.Close()

				err = json.NewDecoder(resp.Body).Decode(&migrationStatus)
				if err != nil {
					t.Fatalf("Failed to decode body, %v", err)
				}

				for _, m := range migrationStatus {
					requestStatus[m.MigrationStatus] += 1
				}
				fmt.Printf("Requests: %d, PENDING: %d, IN_PROGRESS: %d, COMPLETED: %d, FAILED: %d, EXIST_IN_DB: %d, QUEUED: %d, TERM_FAILED: %d\n",
					len(migrationStatus),
					requestStatus[dbs.PENDING],
					requestStatus[dbs.IN_PROGRESS],
					requestStatus[dbs.COMPLETED],
					requestStatus[dbs.FAILED],
					requestStatus[dbs.EXIST_IN_DB],
					requestStatus[dbs.QUEUED],
					requestStatus[dbs.TERM_FAILED])
			}

			if requestStatus[9] > 0 {
				t.Fatalf("%d requests failed\n", requestStatus[9])
			}

			if time.Since(now) > migTimeout {
				t.Fatalf("Migration request timeout")
			}
		})
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
