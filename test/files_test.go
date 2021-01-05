package main

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// TestFiles API
func TestFiles(t *testing.T) {

	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// prepare record for insertion
	rec := make(dbs.Record)
	rec["file_id"] = 0
	rec["logical_file_name"] = "/one/two/three/file.root"
	rec["is_file_valid"] = 1
	rec["dataset_id"] = 0
	rec["block_id"] = 1
	rec["file_type_id"] = 1
	rec["check_sum"] = "123"
	rec["event_count"] = 100
	rec["file_size"] = 12345
	rec["adler32"] = "adler"
	rec["md5"] = "md5"
	rec["auto_cross_section"] = 123.123
	rec["last_modification_date"] = 1607536535
	rec["last_modified_by"] = "Valentin"

	// insert new record
	var api dbs.API
	utils.VERBOSE = 1
	err := api.InsertFiles(rec)
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}

	// fetch this record from DB, here we can either use nil writer
	// or use StdoutWriter instance (defined in test/main.go)
	//     params := make(dbs.Record)
	//     var w http.ResponseWriter
	//     w = StdoutWriter("")
	//     log.Println("Fetch data from Files API")
	//     _, err = api.Files(params, w)
	//     if err != nil {
	//         t.Errorf("Fail to look-up data files %v\n", err)
	//     }
}
