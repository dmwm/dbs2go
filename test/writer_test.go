package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	api := "/primarydatasets"
	hdlr := web.PrimaryDatasetsHandler
	log.Println("insert primary dataset")
	insertData(t, api, "data/primarydataset.json", "primary_ds_name", hdlr)
	log.Println("re-insert primary dataset")
	insertData(t, api, "data/primarydataset.json", "primary_ds_name", hdlr)

	api = "/outputconfigs"
	hdlr = web.OutputConfigsHandler
	log.Println("insert output config")
	insertData(t, api, "data/outputconfig.json", "output_module_label", hdlr)
	log.Println("re-insert output config")
	insertData(t, api, "data/outputconfig.json", "output_module_label", hdlr)

	api = "/acquisitioneras"
	hdlr = web.AcquisitionErasHandler
	log.Println("insert acquisition era")
	insertData(t, api, "data/acquisitionera.json", "acquisition_era_name", hdlr)
	log.Println("re-insert acquisition era")
	insertData(t, api, "data/acquisitionera.json", "acquisition_era_name", hdlr)

	api = "/processingeras"
	hdlr = web.ProcessingErasHandler
	log.Println("insert processing era")
	insertData(t, api, "data/processingera.json", "processing_era_name", hdlr)
	log.Println("re-insert processing era")
	insertData(t, api, "data/processingera.json", "processing_era_name", hdlr)

	api = "/datatiers"
	hdlr = web.DatatiersHandler
	log.Println("insert data tier")
	insertData(t, api, "data/datatier.json", "data_tier_name", hdlr)
	log.Println("re-insert data tier")
	insertData(t, api, "data/datatier.json", "data_tier_name", hdlr)

	api = "/datasetaccesstypes"
	hdlr = web.DatasetAccessTypesHandler
	log.Println("insert dataset access type")
	insertData(t, api, "data/datasetaccesstype.json", "dataset_access_type", hdlr)
	log.Println("re-insert dataset access type")
	insertData(t, api, "data/datasetaccesstype.json", "dataset_access_type", hdlr)

	api = "/physicsgroups"
	hdlr = web.PhysicsGroupsHandler
	log.Println("insert physics group")
	insertData(t, api, "data/physicsgroup.json", "physics_group_name", hdlr)
	log.Println("re-insert physics group")
	insertData(t, api, "data/physicsgroup.json", "physics_group_name", hdlr)

	api = "/datasets"
	hdlr = web.DatasetsHandler
	log.Println("insert dataset")
	insertData(t, api, "data/dataset.json", "dataset", hdlr)
	log.Println("re-insert dataset")
	insertData(t, api, "data/dataset.json", "dataset", hdlr)

	api = "/blocks"
	hdlr = web.BlocksHandler
	log.Println("insert block")
	insertData(t, api, "data/block.json", "block_name", hdlr)
	log.Println("re-insert block")
	insertData(t, api, "data/block.json", "block_name", hdlr)

	api = "/files"
	hdlr = web.FilesHandler
	log.Println("insert files")
	insertData(t, api, "data/files.json", "logical_file_name", hdlr)
	log.Println("re-insert files")
	insertData(t, api, "data/files.json", "logical_file_name", hdlr)

	api = "/fileparents"
	hdlr = web.FileParentsHandler
	log.Println("insert file parents")
	insertData(t, api, "data/files.json", "logical_file_name", hdlr)
	log.Println("re-insert file parents")
	insertData(t, api, "data/files.json", "logical_file_name", hdlr)

	api = "/bulkblocks"
	hdlr = web.BulkBlocksHandler
	log.Println("insert bulk block")
	insertData(t, api, "data/bulkblock.json", "block_name", hdlr)
	log.Println("re-insert bulk block")
	insertData(t, api, "data/bulkblock.json", "block_name", hdlr)
}

// insertData provides a test to insert DBS data
func insertData(t *testing.T, api, dataFile, attr string, hdlr func(http.ResponseWriter, *http.Request)) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// setup HTTP request
	var data []byte
	var err error
	var rr *httptest.ResponseRecorder
	data, err = os.ReadFile(dataFile)
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
	rr, err = respRecorder("POST", api, reader, hdlr)
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
	val, ok := rec[attr]
	if !ok {
		t.Error("unable to extract primary_ds_name from loaded record")
	}
	getApi := fmt.Sprintf("%s?%s=%s", api, attr, val)
	rr, err = respRecorder("GET", getApi, reader, hdlr)
	if err != nil {
		t.Error(err)
	}

	// unmarshal received records
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Printf("reader api %s received data:\n%v", getApi, string(data))
}

// insertOutputConfig provides a test to insert output config data
func insertOutputConfig(t *testing.T) {
}

// insertAcquisitionEra provides a test to insert acquisition era
func insertAcquisitionEra(t *testing.T) {
}
