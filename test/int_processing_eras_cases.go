package main

import (
	"net/http"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// processingeras endpoint tests
// TODO: Rest of test cases
func getProcessingErasTestTable(t *testing.T) EndpointTestCase {
	procErasReq := dbs.ProcessingEras{
		PROCESSING_VERSION: int64(TestData.ProcessingVersion),
		DESCRIPTION:        "this_is_a_test",
		CREATE_BY:          "tester",
	}
	procErasResp := dbs.ProcessingEra{
		ProcessingVersion: int64(TestData.ProcessingVersion),
		CreateBy:          "tester",
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
