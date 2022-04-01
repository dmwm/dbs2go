package main

// this file contains logic for processingeras API
// the HTTP request body is defined by dbs.ProcessingEras struct defined in dbs/processingeras.go
// the HTTP response body is defined by dbs.ProcessingEra struct defined in dbs/bulkblocks.go
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// processingeras endpoint tests
// TODO: Rest of test cases
func getProcessingErasTestTable(t *testing.T) EndpointTestCase {
	procErasReq := dbs.ProcessingEras{
		PROCESSING_VERSION: int64(TestData.ProcessingVersion),
		DESCRIPTION:        "this_is_a_test",
		CREATE_BY:          TestData.CreateBy,
	}
	procErasResp := dbs.ProcessingEra{
		ProcessingVersion: int64(TestData.ProcessingVersion),
		CreateBy:          TestData.CreateBy,
		Description:       "this_is_a_test",
		CreationDate:      0,
	}
	return EndpointTestCase{
		description:     "Test processingeras",
		defaultHandler:  web.ProcessingErasHandler,
		defaultEndpoint: "/dbs/processingeras",
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
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       procErasReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					procErasResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}
