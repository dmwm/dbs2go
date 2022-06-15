package main

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/web"
)

// this file contains logic for fileLumi API
// the basic HTTP response body is defined by fileLumiResponse struct in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

// fileLumi response
type fileLumiResponse struct {
	EventCount      int    `json:"event_count"`
	LogicalFileName string `json:"logical_file_name"`
	LumiSectionNum  int    `json:"lumi_section_num"`
	RunNum          int    `json:"run_num"`
}

// fileLumi chunk test table
func getFileLumiChunkTestTable(t *testing.T) EndpointTestCase {
	var fileLumiResp []Response
	for _, v := range LargeBulkBlocksData.Files {
		for _, fl := range v.FileLumiList {
			flr := fileLumiResponse{
				EventCount:      int(fl.EventCount),
				LogicalFileName: v.LogicalFileName,
				LumiSectionNum:  int(fl.LumiSectionNumber),
				RunNum:          int(fl.RunNumber),
			}
			fileLumiResp = append(fileLumiResp, flr)
		}
	}
	resp := append(fileLumiResp[20:], fileLumiResp[:20]...)
	return EndpointTestCase{
		description:     "Test files GET after file lumi chunk insert",
		defaultHandler:  web.FilesHandler,
		defaultEndpoint: "/dbs/filelumis",
		testCases: []testCase{
			{
				description: "Test files GET after fileLumiChunk exceed",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{LargeBulkBlocksData.Block.BlockName},
				},
				output:   resp,
				respCode: http.StatusOK,
			},
		},
	}
}
