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

// TestDBSWriter provides a test to DBS writer functionality
func TestDBSWriter(t *testing.T) {
	log.Println("insert primary dataset")
	insertPrimaryDataset(t)
	log.Println("re-insert primary dataset")
	insertPrimaryDataset(t)

	log.Println("insert output config")
	insertOutputConfig(t)
	log.Println("re-insert output config")
	insertOutputConfig(t)

	log.Println("insert acquisition era")
	insertAcquisitionEra(t)
	log.Println("re-insert acquisition era")
	insertAcquisitionEra(t)

	log.Println("insert processing era")
	insertProcessingEra(t)
	log.Println("re-insert processing era")
	insertProcessingEra(t)

	log.Println("insert dataset")
	insertDataset(t)
	log.Println("re-insert dataset")
	insertDataset(t)

	log.Println("insert block")
	insertBlock(t)
	log.Println("re-insert block")
	insertBlock(t)

	log.Println("insert files")
	insertFiles(t)
	log.Println("re-insert files")
	insertFiles(t)

	log.Println("insert file parents")
	insertFileParents(t)
	log.Println("re-insert file parents")
	insertFileParents(t)

	log.Println("insert bulk block")
	insertBulkBlock(t)
	log.Println("re-insert bulk block")
	insertBulkBlock(t)
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

// insertOutputConfig provides a test to insert output config data
func insertOutputConfig(t *testing.T) {
}

// insertAcquisitionEra provides a test to insert acquisition era
func insertAcquisitionEra(t *testing.T) {
}

// insertProcessingEra provides a test to insert processing era data
func insertProcessingEra(t *testing.T) {
}

// insertDataset provides a test to insert dataset data
func insertDataset(t *testing.T) {
}

// insertBlock provides a test to insert block data
func insertBlock(t *testing.T) {
}

// insertFiles provides a test to insert files data
func insertFiles(t *testing.T) {
}

// insertFileParents provides a test to insert file parents
func insertFileParents(t *testing.T) {
}

// insertBulkBlock provides a test to insert bulk block data
func insertBulkBlock(t *testing.T) {
}
