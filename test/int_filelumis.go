package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
	diff "github.com/r3labs/diff/v3"
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

// compares received response to expected for filelumis
func verifyFileLumis(t *testing.T, received []dbs.Record, expected []Response) {
	rBytes, err := json.Marshal(received)
	if err != nil {
		t.Fatalf("cannot convert received to json %s", "test.int_filelumis.verifyFileLumis")
	}

	var flr []fileLumiResponse
	json.Unmarshal(rBytes, &flr)
	sort.Slice(flr, func(i, j int) bool {
		return flr[i].LumiSectionNum > flr[j].LumiSectionNum
	})

	eBytes, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("cannot convert expected to json %s", "test.int_filelumis.verifyFileLumis")
	}
	var fle []fileLumiResponse
	json.Unmarshal(eBytes, &fle)
	sort.Slice(fle, func(i, j int) bool {
		return fle[i].LumiSectionNum > fle[j].LumiSectionNum
	})

	c, err := diff.Diff(flr, fle)
	if err != nil {
		t.Fatal(err)
	}

	if len(c) != 0 {
		t.Fatal("Difference in fileLumiResponse")
	}
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
	// resp := append(fileLumiResp[20:], fileLumiResp[:20]...)
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
				output:     fileLumiResp,
				respCode:   http.StatusOK,
				verifyFunc: verifyFileLumis,
			},
		},
	}
}
