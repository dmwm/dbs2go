package main

// this file contains logic for outputconfigs API
// the HTTP request body is defined by dbs.OutputConfigRecord struct defined in dbs/outputconfigs.go
// the HTTP response body is defined by outputConfigResponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// expected response from outputconfigs GET API
type outputConfigResponse struct {
	APP_NAME            string `json:"app_name"`
	RELEASE_VERSION     string `json:"release_version"`
	PSET_HASH           string `json:"pset_hash"`
	PSET_NAME           string `json:"pset_name"`
	GLOBAL_TAG          string `json:"global_tag"`
	OUTPUT_MODULE_LABEL string `json:"output_module_label"`
	CREATION_DATE       int64  `json:"creation_date"`
	CREATE_BY           string `json:"create_by"`
}

// outputconfigs endpoint tests
// TODO: Rest of test cases
func getOutputConfigTestTable(t *testing.T) EndpointTestCase {
	outputConfigReq := dbs.OutputConfigRecord{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           TestData.CreateBy,
		SCENARIO:            "note",
	}
	outputConfigResp := outputConfigResponse{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           TestData.CreateBy,
		CREATION_DATE:       0,
	}
	return EndpointTestCase{
		description:     "Test outputconfigs",
		defaultHandler:  web.OutputConfigsHandler,
		defaultEndpoint: "/dbs/outputconfigs",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				input:       nil,
				params:      nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test bad POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: BadRequest{
					BAD_FIELD: "bad",
				},
				params:   nil,
				output:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       outputConfigReq,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test duplicate POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       outputConfigReq,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					outputConfigResp,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
		},
	}
}
