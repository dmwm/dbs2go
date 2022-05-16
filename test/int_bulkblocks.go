package main

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/web"
)

// this file contains logic for bulkblocks API
// both sequential and concurrent bulkblocks are tested
// HTTP request body data is defined in test/data/integration/bulkblocks_data.json. This is generated using generateBulkBlocksData in test/integration_cases.go
// Sequential bulkblocks data is under the parentBulk and childBulk fields in test/data/integration/bulkblocks_data.json
// Concurrent bulkblocks data is under the parentBulk2 and childBulk2 fields in test/data/integration/bulkblocks_data.json
// bulkblocks_data.json is loaded into BulkBlocksData struct defined in test/integration_cases.go
// the HTTP request body is defined by dbs.BulkBlocks struct defined in dbs/bulkblocks.go
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

// bulkblocks test table
func getBulkBlocksTestTable(t *testing.T) EndpointTestCase {
	return EndpointTestCase{
		description:     "Test bulkblocks",
		defaultHandler:  web.BulkBlocksHandler,
		defaultEndpoint: "/dbs/bulkblocks",
		testCases: []testCase{
			{
				description: "Test POST parent bulkblocks",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       BulkBlocksData.ParentData2,
				params: url.Values{
					"block_name": []string{TestData.ParentStepchainBlock + "2"},
				},
				output:   []Response{},
				handler:  web.FilesHandler,
				respCode: http.StatusOK,
			},
			{
				description: "Test POST child bulkblocks",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       BulkBlocksData.ChildData2,
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock + "2"},
				},
				output:   []Response{},
				handler:  web.FilesHandler,
				respCode: http.StatusOK,
			},
		},
	}
}

// concurrent bulkblocks test table
func getConcurrentBulkBlocksTestTable(t *testing.T) EndpointTestCase {
	return EndpointTestCase{
		description:          "Test bulkblocks",
		defaultHandler:       web.BulkBlocksHandler,
		defaultEndpoint:      "/dbs/bulkblocks",
		concurrentBulkBlocks: true,
		testCases: []testCase{
			{
				description: "Test POST parent bulkblocks",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       BulkBlocksData.ParentData,
				output:      []Response{},
				params: url.Values{
					"block_name": []string{TestData.ParentStepchainBlock},
				},
				handler:  web.FilesHandler,
				respCode: http.StatusOK,
			},
			{
				description: "Test POST child bulkblocks",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       BulkBlocksData.ChildData,
				output:      []Response{},
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
				},
				handler:  web.FilesHandler,
				respCode: http.StatusOK,
			},
		},
	}
}
