package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
	"github.com/vkuznet/dbs2go/web"
)

// helper function to create http test response recorder
// for given HTTP Method, url, reader and DBS web handler
func respRecorder(method, url string, reader io.Reader, hdlr func(http.ResponseWriter, *http.Request)) (*httptest.ResponseRecorder, error) {
	// setup HTTP request
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(hdlr)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		data, e := io.ReadAll(rr.Body)
		if e != nil {
			log.Println("unable to read reasponse body, error:", e)
		}
		log.Printf("handler returned wrong status code: got %v want %v message: %s",
			status, http.StatusOK, string(data))
		return nil, err
	}
	return rr, nil
}

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
	writer := utils.StdoutWriter("")

	// insert new record
	//     var api dbs.API
	api := dbs.API{
		Reader:   reader,
		Writer:   writer,
		CreateBy: createBy,
	}
	err := api.InsertDataTiers()
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}

	// setup HTTP request
	rr, err := respRecorder("GET", "/dbs2go/datatiers", nil, web.DatatiersHandler)
	if err != nil {
		t.Error(err)
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
	rr, err := respRecorder("POST", "/dbs2go/datatiers", reader, web.DatatiersHandler)
	if err != nil {
		t.Error(err)
	}

	// we should not receive anything with POST request
	//     bodyString := rr.Body.String()
	//     if bodyString != "" {
	//         t.Errorf("invalid output of POST request, received %s", bodyString)
	//     }

	// make GET request to get our data
	rr, err = respRecorder("GET", "/dbs2go/datatiers", nil, web.DatatiersHandler)
	if err != nil {
		t.Error(err)
	}

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
	writer := utils.StdoutWriter("")

	// insert new record
	//     var api dbs.API
	api := dbs.API{
		Reader:   reader,
		Writer:   writer,
		CreateBy: createBy,
	}
	err := api.InsertAcquisitionEras()
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}

	// setup HTTP request
	// test existing DBS API
	endDate := 1616109166
	rurl := fmt.Sprintf("/dbs2go/acquisitioneras?end_date=%d&acquisition_era_name=%s", endDate, era)
	rr, err := respRecorder("PUT", rurl, nil, web.AcquisitionErasHandler)
	if err != nil {
		t.Error(err)
	}

	// make GET request to get our data
	rr, err = respRecorder("GET", "/dbs2go/acquisitioneras", nil, web.AcquisitionErasHandler)
	if err != nil {
		t.Error(err)
	}

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
