package main

// this file contains logic for datasetaccesstypes API
// the HTTP request body is defined by dbs.DatasetAccessTypes struct defined in dbs/datasetaccesstypes.go
// the HTTP response body is defined by datasetAccessTypeReponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// expected response from datasetaccesstypes GET API
type datasetAccessTypeResponse struct {
	DATASET_ACCESS_TYPE string `json:"dataset_access_type"`
}

// datasetaccesstypes endpoint tests
func getDatasetAccessTypesTestTable(t *testing.T) EndpointTestCase {
	dataATreq := dbs.DatasetAccessTypes{
		DATASET_ACCESS_TYPE: TestData.DatasetAccessType,
	}
	dataATresp := datasetAccessTypeResponse{
		DATASET_ACCESS_TYPE: TestData.DatasetAccessType,
	}
	return EndpointTestCase{
		description:     "Test datasetaccesstypes",
		defaultHandler:  web.DatasetAccessTypesHandler,
		defaultEndpoint: "/dbs/datasetaccesstypes",
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
				input:       dataATreq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					dataATresp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}
