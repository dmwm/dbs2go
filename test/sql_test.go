package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	yaml "gopkg.in/yaml.v2"
)

// SQLRecord represents API mapping for dbs server
type SQLRecord struct {
	Api          string                   `yaml:"api"`          // DBS API name
	Params       []map[string][]string    `yaml:"params"`       // DBS API parameters
	InsertApi    string                   `yaml:"insertApi"`    // DBS insert API name
	InsertParams []map[string]interface{} `yaml:"insertParams"` // DBS insert API parameters
}

// TestSQL API
func TestSQL(t *testing.T) {

	// initialize DB for testing with DRYRUN=true
	db := initDB(true)
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
	var w http.ResponseWriter
	w = StdoutWriter("")
	for _, rec := range records {
		params := make(dbs.Record)
		for _, rmap := range rec.Params {
			for k, v := range rmap {
				params[k] = v
			}
		}
		log.Printf("SQL test for %s API with params %+v\n", rec.Api, params)
		r := reflect.ValueOf(dbs.API{})
		m := r.MethodByName(rec.Api)
		args := []reflect.Value{reflect.ValueOf(params), reflect.ValueOf(w)}
		output := m.Call(args)
		err := output[1].Interface()
		if err != nil {
			t.Errorf("Fail to look-up data %v\n", err)
		}
	}
}

// TestInsertSQL API
func TestInsertSQL(t *testing.T) {

	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// get list of APIs and their parameters
	fname := "insert.yaml"
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Errorf("Unable to read file %s, error %v\n", fname, err)
	}
	var records []SQLRecord
	err = yaml.Unmarshal([]byte(data), &records)
	if err != nil {
		t.Errorf("Unable to parse file %s, error %v\n", fname, err)
	}

	//     utils.VERBOSE = 2 // be verbose

	// run insert APIs
	for _, rec := range records {

		// create new writer for our test
		var w http.ResponseWriter
		w = StdoutWriter("")

		// insert some info in DBS
		dbs.DRYRUN = true
		params := make(dbs.Record)
		for _, rmap := range rec.InsertParams {
			for k, v := range rmap {
				params[k] = v
			}
		}
		data, _ := json.Marshal(params)
		log.Printf("SQL test for %s API with params %+v\n", rec.InsertApi, string(data))
		r := reflect.ValueOf(dbs.API{})
		m := r.MethodByName(rec.InsertApi)
		args := []reflect.Value{reflect.ValueOf(params)}
		output := m.Call(args)
		//         log.Println("output of insert", output[0].Interface())
		if fmt.Sprintf("%v", output[0].Interface()) != "<nil>" {
			t.Fatalf("Fail to look-up data %v\n", output[0].Interface())
		}

		// skip look-up test
		if rec.Api == "" {
			continue
		}

		// look-up back inserted info
		dbs.DRYRUN = false
		params = make(dbs.Record)
		for _, rmap := range rec.Params {
			for k, v := range rmap {
				params[k] = v
			}
		}
		data, _ = json.Marshal(params)
		log.Printf("SQL test for %s API with params %+v\n", rec.Api, string(data))
		r = reflect.ValueOf(dbs.API{})
		m = r.MethodByName(rec.Api)
		args = []reflect.Value{reflect.ValueOf(params), reflect.ValueOf(w)}
		output = m.Call(args)
		//         log.Println("output of lookup", output[0].Interface(), output[1].Interface())
		if fmt.Sprintf("%v", output[1].Interface()) != "<nil>" {
			t.Fatalf("Fail to look-up data %v\n", output[1].Interface())
		}
	}
}
