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
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestMigration tests DBS Migration process
func TestIntMigration(t *testing.T) {
	db := initDB(false)
	defer db.Close()

	// start DBSReader server from which we will read the data for migration process
	base := "dbs-one-reader"
	srv1 := dbsServer(t, base, "DBS_DB_FILE_1", "DBSReader", true, 500)
	checkServer(t, srv1.URL, base)

	// start DBSWriter server to which we will write the data
	base = "dbs-one-writer"
	srv2 := dbsServer(t, base, "DBS_DB_FILE_1", "DBSWriter", true, 500)
	checkServer(t, srv2.URL, base)

	// start DBSReader server from which we will read the data after migration process
	base = "dbs-two-reader"
	srv3 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSReader", true, 500)
	checkServer(t, srv3.URL, base)

	// start DBSWriter server to which we will write the data during migration process
	base = "dbs-two-writer"
	srv4 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSWriter", true, 500)
	checkServer(t, srv4.URL, base)

	// start DBSMigrate server to which we will post migration requests
	base = "dbs-migrate"
	srv5 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSMigrate", true, 500)
	checkServer(t, srv5.URL, base)

	// start DBSMigration server which will process migration requests
	base = "dbs-migration"
	srv6 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSMigration", true, 500)
	checkServer(t, srv6.URL, base)

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
	req := newreq(t, "POST", srv2.URL, "/dbs-one-writer/bulkblocks", reader, nil, headers)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Bulkblocks insert failed!")
	}
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
