package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	validator "github.com/go-playground/validator/v10"
	"github.com/vkuznet/dbs2go/dbs"
)

// TestValidator
func TestValidator(t *testing.T) {
	// set DBS lexicon patterns
	lexiconFile := os.Getenv("DBS_LEXICON_FILE")
	if lexiconFile == "" {
		t.Error(errors.New("Please setup DBS_LEXICON_FILE env"))
	}
	lexPatterns, err := dbs.LoadPatterns(lexiconFile)
	dbs.LexiconPatterns = lexPatterns

	var req *http.Request
	host := "http://localhost:8111/dbs2go"
	rurl := host + "/primarydatasets?primary_ds_name=*"
	req, _ = http.NewRequest("GET", rurl, nil)
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
	rurl = host + "/primarydatasets?primary_ds_name=bla*"
	req, _ = http.NewRequest("GET", rurl, nil)
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
	rurl = host + "/datasets?dataset=/unittest_web_primary_ds_name*/*/*"
	req, _ = http.NewRequest("GET", rurl, nil)
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
	//     rurl = host + "/datasets?dataset=/*/*/*"
	//     req, _ = http.NewRequest("GET", rurl, nil)
	//     err = dbs.Validate(req)
	//     if err != nil {
	//         t.Error(err)
	//     }
}

// TestValidatePostPayload
func TestValidatePostPayload(t *testing.T) {
	var req *http.Request
	var err error
	host := "http://localhost:8111/dbs2go"
	rurl := host + "/datatiers?data_tier_name=*"
	rec := make(map[string]string)
	rec["data_tier_name"] = "RAW"
	data, err := json.Marshal(rec)
	if err != nil {
		t.Error(err)
	}
	req, _ = http.NewRequest("POST", rurl, bytes.NewBuffer(data))
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
}

// TestDBRecordValidator
func TestDBRecordValidator(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	cby := "test"
	tier := "raw"
	rec := dbs.DataTiers{DATA_TIER_NAME: tier, CREATION_DATE: ts, CREATE_BY: cby}
	// this should fail by design since we provide tier in lower-case
	err := rec.Validate()
	if err == nil {
		t.Error("No error is raised when we request validation")
	} else {
		log.Println("WE SHOULD GET ERROR message from Validator =>", err)
	}
	rec = dbs.DataTiers{DATA_TIER_NAME: tier, CREATION_DATE: ts}
	// this should fail by design since we do not provide create by value
	err = rec.Validate()
	if err == nil {
		t.Error("No error is raised when we request validation")
	} else {
		log.Println("WE SHOULD GET ERROR message from Validator =>", err)
	}
	tier = "RAW"
	rec = dbs.DataTiers{DATA_TIER_NAME: tier, CREATION_DATE: ts, CREATE_BY: cby}
	err = rec.Validate()
	// now validation should pass
	if err != nil {
		t.Error(err)
	}
}
