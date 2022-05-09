package main

import (
	"net/http"
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
