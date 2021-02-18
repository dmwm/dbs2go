package main

import (
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
