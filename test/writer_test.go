package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// TestInjectDBSData provides a test to insert DBS data
func TestInjectDBSData(t *testing.T) {
	log.Println("insert primary dataset")
	// insert primary dataset
	insertPrimaryDataset(t)
	// re-try should make no harm
	log.Println("re-insert primary dataset")
	insertPrimaryDataset(t)
}

// insertPrimaryDataset provides a test to insert primary dataset data
func insertPrimaryDataset(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// setup HTTP request
	var data []byte
	var err error
	var rr *httptest.ResponseRecorder
	data, err = os.ReadFile("data/primarydataset.json")
	if err != nil {
		t.Error(err.Error())
	}
	var rec dbs.Record
	err = json.Unmarshal(data, &rec)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	reader := bytes.NewReader(data)

	// test writer POST DBS API
	api := "/dbs2go-writer/primarydatasets"
	rr, err = respRecorder("POST", api, reader, web.PrimaryDatasetsHandler)
	if err != nil {
		t.Error(err)
	}

	log.Printf("writer api %s send data:\n%v", api, string(data))

	// unmarshal received records
	var records []dbs.Record
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}

	// we should receive empty list
	emptyList := "[]"
	if len(records) != 0 && string(data) != emptyList {
		t.Errorf("writer POST api %s, wrong output %v", api, string(data))
	}

	// test reader GET DBS API
	primds, ok := rec["primary_ds_name"]
	if !ok {
		t.Error("unable to extract primary_ds_name from loaded record")
	}
	api = fmt.Sprintf("/dbs2go/primarydatasets?primary_ds_name=%s", primds)
	rr, err = respRecorder("GET", api, reader, web.PrimaryDatasetsHandler)
	if err != nil {
		t.Error(err)
	}

	// unmarshal received records
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Printf("reader api %s received data:\n%v", api, string(data))
}
