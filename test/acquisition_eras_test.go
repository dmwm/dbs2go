package main

import (
	"log"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// TestAcquisitionEras API
func TestAcquisitionEras(t *testing.T) {

	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// prepare record for insertion
	rec := make(dbs.Record)
	rec["acquisition_era_id"] = 0
	rec["acquisition_era_name"] = "era"
	rec["start_date"] = 123
	rec["end_date"] = 321
	rec["creation_date"] = 123
	rec["create_by"] = "tester"
	rec["description"] = "description"

	// insert new record
	var api dbs.API
	utils.VERBOSE = 1
	err := api.InsertAcquisitionEras(rec)
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}

	// fetch this record from DB, here we can either use nil writer
	// or use StdoutWriter instance (defined in test/main.go)
	params := make(dbs.Record)
	var w http.ResponseWriter
	w = StdoutWriter("")
	log.Println("Fetch data from AcquisitionEras API")
	_, err = api.AcquisitionEras(params, w)
	if err != nil {
		t.Errorf("Fail to look-up data tiers %v\n", err)
	}
}

// TestAcquisitionErasSQL API
func TestAcquisitionErasSQL(t *testing.T) {
	// initialize DB for testing
	db := initDB(true) // init DB with dryRun mode
	defer db.Close()
	var api dbs.API
	params := make(dbs.Record)
	var w http.ResponseWriter
	w = StdoutWriter("")
	log.Println("Test AcquisitionEras API statement")
	api.AcquisitionEras(params, w)
}

// TestAcquisitionErasCiSQL API
func TestAcquisitionErasCiSQL(t *testing.T) {
	// initialize DB for testing
	db := initDB(true) // init DB with dryRun mode
	defer db.Close()
	var api dbs.API
	params := make(dbs.Record)
	var w http.ResponseWriter
	w = StdoutWriter("")
	log.Println("Test AcquisitionErasCi API statement")
	api.AcquisitionErasCi(params, w)
}
