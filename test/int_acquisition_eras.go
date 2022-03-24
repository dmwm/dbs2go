package main

// this file contains logic for acquisitioneras API
// the HTTP requests body is defined by dbs.AcquisitionEras struct defined in dbs/acquisitioneras.go
// the HTTP response body is defined by dbs.AcquisitionEra struct defined in dbs/bulkblocks.go
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// acquisitioneras endpoint tests
// TODO: Rest of test cases
func getAcquisitionErasTestTable(t *testing.T) EndpointTestCase {
	acqEraReq := dbs.AcquisitionEras{
		ACQUISITION_ERA_NAME: TestData.AcquisitionEra,
		DESCRIPTION:          "note",
		CREATE_BY:            "tester",
	}
	acqEraResp := dbs.AcquisitionEra{
		AcquisitionEraName: TestData.AcquisitionEra,
		StartDate:          0,
		EndDate:            0,
		CreationDate:       0,
		CreateBy:           "tester",
		Description:        "note",
	}
	return EndpointTestCase{
		description:     "Test acquisitioneras",
		defaultHandler:  web.AcquisitionErasHandler,
		defaultEndpoint: "/dbs/acquisitioneras",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       acqEraReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					acqEraResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}