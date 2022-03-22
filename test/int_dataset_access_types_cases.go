package main

import (
	"net/http"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// expected response from datasetaccesstypes GET API
type datasetAccessTypeResponse struct {
	DATASET_ACCESS_TYPE string `json:"dataset_access_type"`
}

// datasetaccesstypes endpoint tests
func getDatasetAccessTypesTestTable(t *testing.T) EndpointTestCase {
	dataATreq := dbs.DatasetAccessTypes{
		DATASET_ACCESS_TYPE: "PRODUCTION",
	}
	dataATresp := datasetAccessTypeResponse{
		DATASET_ACCESS_TYPE: "PRODUCTION",
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
