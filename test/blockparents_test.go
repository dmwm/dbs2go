package main

import (
	"log"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
)

// TestBlockParentsSQL API
func TestBlockParentsSQL(t *testing.T) {
	// initialize DB for testing
	db := initDB(true) // init DB with dryRun mode
	defer db.Close()
	var api dbs.API
	params := make(dbs.Record) // the Record is map[string]interface{}
	params["Owner"] = "sqlite"
	params["logical_file_name"] = []string{"/path/file.root"} // pass params as list as in HTTP
	var w http.ResponseWriter
	w = StdoutWriter("")
	log.Println("Test BlockParents API statement with logical_file_name parameter")
	_, err := api.BlockParents(params, w)
	if err != nil {
		t.Errorf("Fail in BlockParents, error %v\n", err)
	}

	params = make(dbs.Record)
	params["Owner"] = "sqlite"
	params["dataset"] = []string{"/a/b/c"}
	params["run_num"] = []string{"1", "2", "3"}
	log.Println("Test BlockParents API statement with dataset parameter")
	_, err = api.BlockParents(params, w)
	if err != nil {
		t.Errorf("Fail in BlockParents, error %v\n", err)
	}
}
