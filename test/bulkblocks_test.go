package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
)

// TestBulkBlocks API
func TestBulkBlocks(t *testing.T) {

	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// read bulkblocks.json from test area and process it
	dir, err := os.Getwd()
	if err != nil {
		t.Errorf("Fail to get current directory %v\n", err)
	}
	fname := fmt.Sprintf("%s/bulkblocks.json", dir)
	file, err := os.Open(fname)
	if err != nil {
		t.Errorf("Fail to open file %s, error %v\n", fname, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var api dbs.API
	err = api.BulkBlocks(decoder)
	if err != nil {
		t.Errorf("Fail to process bulkblocks data %v\n", err)
	}
}
