package main

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/web"
)

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
				input:       BulkBlocksData.ParentData,
				params: url.Values{
					"block_name": []string{TestData.ParentStepchainBlock},
				},
				output:   []Response{},
				handler:  web.FilesHandler,
				respCode: http.StatusOK,
			},
			{
				description: "Test POST child bulkblocks",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       BulkBlocksData.ChildData,
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
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
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST child bulkblocks",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       BulkBlocksData.ChildData,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
		},
	}
}
