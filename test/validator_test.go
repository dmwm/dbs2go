package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
)

// TestValidator
func TestValidator(t *testing.T) {
	var req *http.Request
	var err error
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
	rurl = host + "/datasets?dataset=/*/*/*"
	req, _ = http.NewRequest("GET", rurl, nil)
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
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
