package main

import (
	"log"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// TestPhysicsGroups API
func TestPhysicsGroups(t *testing.T) {

	// initialize DB for testing
	db := initDB()
	defer db.Close()

	// prepare record for insertion
	rec := make(dbs.Record)
	rec["physics_group_id"] = 0
	rec["physics_group_name"] = "phys"

	// insert new record
	var api dbs.API
	utils.VERBOSE = 1
	err := api.InsertPhysicsGroups(rec)
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}

	// fetch this record from DB, here we can either use nil writer
	// or use StdoutWriter instance (defined in test/main.go)
	params := make(dbs.Record)
	var w http.ResponseWriter
	w = StdoutWriter("")
	log.Println("Fetch data from PhysicsGroups API")
	_, err = api.PhysicsGroups(params, w)
	if err != nil {
		t.Errorf("Fail to look-up data tiers %v\n", err)
	}
}
