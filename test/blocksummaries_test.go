package main

import (
	"log"
	"net/http"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
)

// TestBlockSummariesSQL API
func TestBlockSummariesSQL(t *testing.T) {
	// initialize DB for testing
	db := initDB(true) // init DB with dryRun mode
	defer db.Close()
	var api dbs.API
	params := make(dbs.Record)
	params["Owner"] = "sqlite"
	params["dataset"] = []string{"/a/b/c"}
	params["run_num"] = []string{"1", "2", "3"}
	var w http.ResponseWriter
	w = StdoutWriter("")
	log.Println("Test BlockSummaries API statement with dataset parameter")
	_, err := api.BlockSummaries(params, w)
	if err != nil {
		t.Errorf("Fail in BlockSummaries, error %v\n", err)
	}
}
