package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// TestDBSWriter provides a test to DBS writer functionality
func TestDBSWriter(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	var err error

	api := "/primarydatasets"
	hdlr := web.PrimaryDatasetsHandler
	log.Println("insert primary dataset")
	insertData(t, db, "POST", api, "data/primarydataset.json", "primary_ds_name", hdlr)
	log.Println("re-insert primary dataset")
	insertData(t, db, "POST", api, "data/primarydataset.json", "primary_ds_name", hdlr)

	api = "/outputconfigs"
	hdlr = web.OutputConfigsHandler
	log.Println("insert output config")
	insertData(t, db, "POST", api, "data/outputconfig.json", "output_module_label", hdlr)
	log.Println("re-insert output config")
	insertData(t, db, "POST", api, "data/outputconfig.json", "output_module_label", hdlr)

	api = "/acquisitioneras"
	hdlr = web.AcquisitionErasHandler
	log.Println("insert acquisition era")
	insertData(t, db, "POST", api, "data/acquisitionera.json", "acquisition_era_name", hdlr)
	log.Println("re-insert acquisition era")
	insertData(t, db, "POST", api, "data/acquisitionera.json", "acquisition_era_name", hdlr)

	api = "/processingeras"
	hdlr = web.ProcessingErasHandler
	log.Println("insert processing era")
	insertData(t, db, "POST", api, "data/processingera.json", "processing_version", hdlr)
	log.Println("re-insert processing era")
	insertData(t, db, "POST", api, "data/processingera.json", "processing_version", hdlr)

	api = "/datatiers"
	hdlr = web.DatatiersHandler
	log.Println("insert data tier")
	insertData(t, db, "POST", api, "data/datatier.json", "data_tier_name", hdlr)
	log.Println("re-insert data tier")
	insertData(t, db, "POST", api, "data/datatier.json", "data_tier_name", hdlr)

	api = "/datasetaccesstypes"
	hdlr = web.DatasetAccessTypesHandler
	log.Println("insert dataset access type")
	insertData(t, db, "POST", api, "data/datasetaccesstype.json", "dataset_access_type", hdlr)
	log.Println("re-insert dataset access type")
	insertData(t, db, "POST", api, "data/datasetaccesstype.json", "dataset_access_type", hdlr)

	api = "/physicsgroups"
	hdlr = web.PhysicsGroupsHandler
	log.Println("insert physics group")
	insertData(t, db, "POST", api, "data/physicsgroup.json", "physics_group_name", hdlr)
	log.Println("re-insert physics group")
	insertData(t, db, "POST", api, "data/physicsgroup.json", "physics_group_name", hdlr)

	api = "/datasets"
	hdlr = web.DatasetsHandler
	log.Println("insert dataset")
	insertData(t, db, "POST", api, "data/dataset.json", "dataset", hdlr)
	log.Println("re-insert dataset")
	insertData(t, db, "POST", api, "data/dataset.json", "dataset", hdlr)

	api = "/blocks"
	hdlr = web.BlocksHandler
	log.Println("insert block")
	insertData(t, db, "POST", api, "data/block.json", "block_name", hdlr)
	log.Println("re-insert block")
	insertData(t, db, "POST", api, "data/block.json", "block_name", hdlr)

	// use datasetparent.json to insert dataset record used by datasetparent.json
	api = "/datasets"
	hdlr = web.DatasetsHandler
	log.Println("insert parent dataset")
	insertData(t, db, "POST", api, "data/datasetparent.json", "dataset", hdlr)
	log.Println("re-insert parent dataset")
	insertData(t, db, "POST", api, "data/datasetparent.json", "dataset", hdlr)

	// use blockparent.json to insert block record used by fileparent.json
	api = "/blocks"
	hdlr = web.BlocksHandler
	log.Println("insert block")
	insertData(t, db, "POST", api, "data/blockparent.json", "block_name", hdlr)
	log.Println("re-insert block")
	insertData(t, db, "POST", api, "data/blockparent.json", "block_name", hdlr)

	// use fileparent.json to insert file records which later will be used
	// as file parents in subsequent call with files.json
	api = "/files"
	hdlr = web.FilesHandler
	log.Println("insert file parents")
	insertData(t, db, "POST", api, "data/fileparent.json", "", hdlr)
	log.Println("re-insert file parents")
	insertData(t, db, "POST", api, "data/fileparent.json", "", hdlr)

	api = "/files"
	hdlr = web.FilesHandler
	log.Println("insert files")
	insertData(t, db, "POST", api, "data/file.json", "", hdlr)
	log.Println("re-insert files")
	insertData(t, db, "POST", api, "data/file.json", "", hdlr)

	api = "/bulkblocks"
	hdlr = web.BulkBlocksHandler
	log.Println("insert bulk block")
	insertData(t, db, "POST", api, "data/bulkblock.json", "", hdlr)
	log.Println("re-insert bulk block")
	insertData(t, db, "POST", api, "data/bulkblock.json", "", hdlr)

	api = "/files"
	hdlr = web.FilesHandler
	log.Println("update files")
	insertData(t, db, "PUT", api, "data/updatefile.json", "", hdlr)

	t.Logf("finish DBS writer test")
	err = db.Close()
	if err != nil {
		t.Error(err.Error())
	}
}

// insertData provides a test to insert DBS data
func insertData(t *testing.T, db *sql.DB, method, api, dataFile, attr string, hdlr func(http.ResponseWriter, *http.Request)) {
	// setup HTTP request
	var data []byte
	var err error
	var rr *httptest.ResponseRecorder
	data, err = os.ReadFile(dataFile)
	if err != nil {
		log.Printf("ERROR: unable to read %s error %v", dataFile, err.Error())
		t.Fatal(err.Error())
	}
	var rec dbs.Record
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Printf("unable to unmarshal received data into dbs.Record, error %v, try next []dbs.Record", err)
		// let's try to load list of records
		var rrr []dbs.Record
		err = json.Unmarshal(data, &rrr)
		if err != nil {
			t.Fatalf("ERROR: unable to unmarshal received data '%s', error %v", string(data), err)
		}
		log.Println("succeed to load record as []dbs.Record")
	}
	reader := bytes.NewReader(data)

	// test writer DBS API
	rr, err = respRecorder(method, api, reader, hdlr)
	if err != nil {
		log.Printf("ERROR: unable to make %s HTTP request with api=%s, error %v", method, api, err)
		t.Fatal(err)
	}

	log.Printf("writer api %s send data:\n%v", api, string(data))

	// unmarshal received records
	var records []dbs.Record
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Fatalf("ERROR: unable to unmarshal received data '%s', error %v", string(data), err)
	}

	log.Printf("writer api %s returns %v, len(records)=%d", api, string(data), len(records))

	// we should receive empty list
	emptyList := "[]"
	if len(records) != 0 && string(data) != emptyList {
		t.Fatalf("ERROR: writer POST api %s, wrong output %v", api, string(data))
	}

	// if no attribute is provided we'll skip GET API test
	if attr == "" {
		log.Println("skip get API call since no attr is provided")
		return
	}
	// test reader GET DBS API
	val, ok := rec[attr]
	if !ok {
		t.Fatalf("ERROR: unable to extract %s from loaded record", attr)
	}
	var value string
	switch v := val.(type) {
	case string:
		value = url.QueryEscape(v)
	default:
		value = fmt.Sprintf("%v", v)
	}
	getApi := fmt.Sprintf("%s?%s=%s", api, attr, value)
	rr, err = respRecorder("GET", getApi, reader, hdlr)
	if err != nil {
		log.Printf("ERROR: unable to place GET HTTP request with api=%s, error %v", getApi, err)
		t.Fatal(err)
	}

	// unmarshal received records
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Fatalf("ERROR: unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Printf("reader api %s received data:\n%v", getApi, string(data))
}
