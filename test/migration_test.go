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
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/utils"
	"github.com/dmwm/dbs2go/web"
	_ "github.com/mattn/go-sqlite3"
)

// helper funtion to return DBS server with basic parameters
func dbsServer(t *testing.T, base, dbFile, serverType string) *httptest.Server {
	dbfile := os.Getenv(dbFile)
	if dbfile == "" {
		log.Fatal(fmt.Sprintf("no %s env variable, please define", dbFile))
	}

	var lexiconFile string

	if serverType == "DBSReader" {
		lexiconFile = os.Getenv("DBS_READER_LEXICON_FILE")
		if lexiconFile == "" {
			log.Fatal("no DBS_READER_LEXICON_FILE env variable, please define")
		}
	} else {
		lexiconFile = os.Getenv("DBS_WRITER_LEXICON_FILE")
		if lexiconFile == "" {
			log.Fatal("no DBS_WRITER_LEXICON_FILE env variable, please define")
		}
	}

	web.Config.Base = base
	web.Config.DBFile = dbfile
	web.Config.LexiconFile = lexiconFile
	web.Config.ServerCrt = ""
	web.Config.ServerKey = ""
	web.Config.ServerType = serverType
	web.Config.LogFile = fmt.Sprintf("/tmp/dbs2go-%s.log", base)
	web.Config.Verbose = 0
	utils.VERBOSE = 0
	utils.BASE = base
	lexPatterns, err := dbs.LoadPatterns(lexiconFile)
	if err != nil {
		t.Fatal(err)
	}
	dbs.LexiconPatterns = lexPatterns

	initTestLimiter(t, "100-S")

	ts := httptest.NewServer(web.Handlers())

	return ts
}

// TestMigration tests DBS Migration process
func TestMigration(t *testing.T) {
	db := initDB(false)
	defer db.Close()

	// start DBSReader server from which we will read the data for migration process
	base := "dbs-one-reader"
	srv1 := dbsServer(t, base, "DBS_DB_FILE_1", "DBSReader")
	checkServer(t, srv1.URL, base)

	// start DBSWriter server to which we will write the data
	base = "dbs-one-writer"
	srv2 := dbsServer(t, base, "DBS_DB_FILE_1", "DBSWriter")
	checkServer(t, srv2.URL, base)

	// start DBSReader server from which we will read the data after migration process
	base = "dbs-two-reader"
	srv3 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSReader")
	checkServer(t, srv3.URL, base)

	// start DBSWriter server to which we will write the data durign migration process
	base = "dbs-two-writer"
	srv4 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSWriter")
	checkServer(t, srv4.URL, base)

	// start DBSMigrate server to which we will post migration requests
	base = "dbs-migrate"
	srv5 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSMigrate")
	checkServer(t, srv5.URL, base)

	// start DBSMigration server which will process migration requests
	base = "dbs-migration"
	srv6 := dbsServer(t, base, "DBS_DB_FILE_2", "DBSMigration")
	checkServer(t, srv6.URL, base)
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
