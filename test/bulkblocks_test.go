package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
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
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Errorf("Fail to read file %s, error %v\n", fname, err)
	}
	var api dbs.API
	reader := bytes.NewReader(data)
	_, err = api.InsertBulkBlocks(reader)
	if err != nil {
		t.Errorf("Fail to process bulkblocks data %v\n", err)
	}
}
