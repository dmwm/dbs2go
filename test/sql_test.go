package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/utils"
	yaml "gopkg.in/yaml.v2"
)

// SQLRecord represents API mapping for dbs server
type SQLRecord struct {
	Api          string                 `yaml:"api"`          // DBS API name
	Params       []map[string][]string  `yaml:"params"`       // DBS API parameters
	InsertApi    string                 `yaml:"insertApi"`    // DBS insert API name
	InsertParams map[string]interface{} `yaml:"insertParams"` // DBS insert API parameters
}

// TestSQL API
func TestSQL(t *testing.T) {

	// initialize DB for testing with DRYRUN=true
	dburi := os.Getenv("DBS_DB_FILE")
	if dburi == "" {
		log.Fatal("DBS_DB_FILE not defined")
	}
	db := initDB(true, dburi)
	defer db.Close()

	// get list of APIs and their parameters
	fname := "sql.yaml"
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Errorf("Unable to read file %s, error %v\n", fname, err)
	}
	var records []SQLRecord
	err = yaml.Unmarshal([]byte(data), &records)
	if err != nil {
		t.Errorf("Unable to parse file %s, error %v\n", fname, err)
	}

	// fetch this record from DB, here we can either use nil writer
	// or use StdoutWriter instance (defined in test/main.go)
	w := utils.StdoutWriter("")
	sep := ",\n"
	for _, rec := range records {
		params := make(dbs.Record)
		for _, rmap := range rec.Params {
			for k, v := range rmap {
				params[k] = v
			}
		}
		log.Printf("SQL test for %s API with params %+v\n", rec.Api, params)
		r := reflect.ValueOf(&dbs.API{Params: params, Separator: sep, Writer: w})
		m := r.MethodByName(rec.Api)
		//         args := []reflect.Value{reflect.ValueOf(params), reflect.ValueOf(sep), reflect.ValueOf(w)}
		args := []reflect.Value{}
		output := m.Call(args)
		// output represents dbs API output (the error)
		err := output[0].Interface()
		if err != nil {
			t.Errorf("Fail to look-up data %v\n", err)
		}
	}
}

// TestInsertSQL API
func TestInsertSQL(t *testing.T) {

	// initialize DB for testing
	dburi := os.Getenv("DBS_DB_FILE")
	if dburi == "" {
		log.Fatal("DBS_DB_FILE not defined")
	}
	db := initDB(false, dburi)
	defer db.Close()

	// get list of APIs and their parameters
	fname := "data/insert.yaml"
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Errorf("Unable to read file %s, error %v\n", fname, err)
	}
	var records []SQLRecord
	err = yaml.Unmarshal([]byte(data), &records)
	if err != nil {
		t.Errorf("Unable to parse file %s, error %v\n", fname, err)
	}

	utils.VERBOSE = 2 // be verbose
	sep := ",\n"

	// run insert APIs
	for _, rec := range records {
		// create reader from our record and pass it around to insert APIs
		data, _ := json.Marshal(rec.InsertParams)
		reader := bytes.NewReader(data)
		cby := "test"

		// create new writer for our test
		w := utils.StdoutWriter("")

		// insert some info in DBS
		dbs.DRYRUN = true
		log.Printf("SQL test for %s API with params %+v\n", rec.InsertApi, string(data))
		r := reflect.ValueOf(&dbs.API{Reader: reader, CreateBy: cby, Writer: w})
		m := r.MethodByName(rec.InsertApi)
		//         args := []reflect.Value{reflect.ValueOf(reader), reflect.ValueOf(cby)}
		args := []reflect.Value{}
		// call to InsertXXX DBS API, it returns output which consists
		// of two values, the size of inserted record and the error
		output := m.Call(args)
		// output here represent array of return values
		// in this call the insert API returns only error, therefore
		// we look-up it from output[0]
		if fmt.Sprintf("%v", output[0].Interface()) != "<nil>" {
			t.Fatalf("Fail to insert data %v\n", output[1].Interface())
		}

		// skip look-up test
		if rec.Api == "" {
			continue
		}

		// look-up back inserted info
		dbs.DRYRUN = false
		params := make(dbs.Record)
		for _, rmap := range rec.Params {
			for k, v := range rmap {
				params[k] = v
			}
		}
		data, _ = json.Marshal(params)
		log.Printf("SQL test for %s API with params %+v\n", rec.Api, string(data))
		r = reflect.ValueOf(&dbs.API{Params: params, Separator: sep, Writer: w})
		m = r.MethodByName(rec.Api)
		//         args = []reflect.Value{reflect.ValueOf(params), reflect.ValueOf(sep), reflect.ValueOf(w)}
		args = []reflect.Value{}
		output = m.Call(args)
		// output represents dbs API output (the error)
		if fmt.Sprintf("%v", output[0].Interface()) != "<nil>" {
			t.Fatalf("Fail to look-up data %v\n", output[1].Interface())
		}
	}
}
