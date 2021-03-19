package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// TestHTTPGet provides test of GET method for our service
func TestHTTPGet(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// add new record to DB that we will query via HTTP request
	createBy := "test"
	rec := make(dbs.Record)
	rec["data_tier_name"] = "RAW-TEST-0"
	rec["creation_date"] = float64(1607536535)
	rec["create_by"] = createBy
	data, _ := json.Marshal(rec)
	reader := bytes.NewReader(data)

	// insert new record
	var api dbs.API
	err := api.InsertDataTiers(reader, createBy)
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}

	// setup HTTP request
	req, err := http.NewRequest("GET", "/dbs2go/datatiers", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Accept", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(web.LoggingHandler(web.DatatiersHandler))
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// unmarshal received records
	var records []dbs.Record
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	for _, rrr := range records {
		for k, v := range rec {
			if v != rrr[k] {
				t.Errorf("mismatch of inserted and received records, key %s, inserted value %v received value %v", k, v, rrr[k])
			}
		}
	}
}

// TestHTTPPost provides test of GET method for our service
func TestHTTPPost(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// setup HTTP request
	data := []byte(`{"data_tier_name":"TEST-POST-RAW"}`)
	reader := bytes.NewReader(data)

	// test existing DBS API
	req, err := http.NewRequest("POST", "/dbs2go/datatiers", reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(web.LoggingHandler(web.DatatiersHandler))
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// we should not receive anything with POST request
	if rr.Body.String() != "" {
		t.Errorf("invalid output of POST request")
	}

	// make GET request to get our data
	req, err = http.NewRequest("GET", "/dbs2go/datatiers", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Accept", "application/json")

	rr = httptest.NewRecorder()
	handler = http.HandlerFunc(web.LoggingHandler(web.DatatiersHandler))
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// we should obtain the following output
	// [{...}, {....}]
	// unmarshal received records
	var records []dbs.Record
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Println("Received data", string(data))
	found := false
	for _, rrr := range records {
		tier := rrr["data_tier_name"]
		if tier == "TEST-POST-RAW" {
			found = true
		}
	}
	if !found {
		t.Errorf("data tier is not found after POST request")
	}
}

// TestHTTPPut
func TestHTTPPut(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// insert record in DB
	createBy := "test"
	date := 1607536535
	era := "ERA"
	rec := make(dbs.Record)
	rec["acquisition_era_name"] = era
	rec["creation_date"] = float64(date)
	rec["create_by"] = createBy
	rec["start_date"] = float64(date)
	data, _ := json.Marshal(rec)
	reader := bytes.NewReader(data)

	// insert new record
	var api dbs.API
	err := api.InsertAcquisitionEras(reader, createBy)
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}

	// setup HTTP request
	// test existing DBS API
	endDate := 1616109166
	rurl := fmt.Sprintf("/dbs2go/acquisitioneras?end_date=%d&acquisition_era_name=%s", endDate, era)
	req, err := http.NewRequest("PUT", rurl, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Accept", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(web.LoggingHandler(web.AcquisitionErasHandler))
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// make GET request to get our data
	req, err = http.NewRequest("GET", "/dbs2go/acquisitioneras", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Accept", "application/json")

	rr = httptest.NewRecorder()
	handler = http.HandlerFunc(web.LoggingHandler(web.AcquisitionErasHandler))
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// we should obtain the following output
	// [{...}, {....}]
	// unmarshal received records
	var records []dbs.Record
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Println("Received data", string(data))
	found := false
	for _, rrr := range records {
		if rrr["acquisition_era_name"] == era {
			found = true
			val := fmt.Sprintf("%v", rrr["end_date"])
			exp := fmt.Sprintf("%v", float64(endDate))
			log.Println("end_date received", val, "expected", exp)
			if val != exp {
				t.Errorf("era end_date was not updated in PUT request")
			}
		}
	}
	if !found {
		t.Errorf("acquisition era is not found after GET request")
	}
}
