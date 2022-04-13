package main

// this file contains logic for the blocks API
// the HTTP requests body is defined by dbs.Blocks struct defined in dbs/blocks.go
// the HTTP response body is defined by blockResponse struct defined in this file
// the HTTP response body for the `detail` query is defined by blockDetailResponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// normal blocks API response
type blockResponse struct {
	BLOCK_NAME string `json:"block_name"`
}

// detailed blocks API response
type blockDetailResponse struct {
	BlockID              int64  `json:"block_id"`
	DatasetID            int64  `json:"dataset_id"`
	CreateBy             string `json:"create_by"`
	CreationDate         int64  `json:"creation_date"`
	Dataset              string `json:"dataset"`
	OpenForWriting       int64  `json:"open_for_writing"`
	BlockName            string `json:"block_name"`
	FileCount            int64  `json:"file_count"`
	OriginSiteName       string `json:"origin_site_name"`
	BlockSize            int64  `json:"block_size"`
	LastModifiedBy       string `json:"last_modified_by"`
	LastModificationDate int64  `json:"last_modification_date"`
}

// blocks endpoint tests
func getBlocksTestTable(t *testing.T) EndpointTestCase {
	blockReq := dbs.Blocks{
		BLOCK_NAME:       TestData.Block,
		ORIGIN_SITE_NAME: TestData.Site,
		CREATE_BY:        TestData.CreateBy,
		LAST_MODIFIED_BY: TestData.CreateBy,
	}
	parentBlockReq := dbs.Blocks{
		BLOCK_NAME:       TestData.ParentBlock,
		ORIGIN_SITE_NAME: TestData.Site,
		CREATE_BY:        TestData.CreateBy,
		LAST_MODIFIED_BY: TestData.CreateBy,
	}
	blockResp := blockResponse{
		BLOCK_NAME: TestData.Block,
	}
	blockParentResp := blockResponse{
		BLOCK_NAME: TestData.ParentBlock,
	}
	blockDetailResp := blockDetailResponse{
		BlockID:              1,
		BlockName:            TestData.Block,
		BlockSize:            0,
		CreateBy:             TestData.CreateBy,
		CreationDate:         0,
		Dataset:              TestData.Dataset,
		DatasetID:            1,
		FileCount:            0,
		LastModificationDate: 0,
		LastModifiedBy:       TestData.CreateBy,
		OpenForWriting:       0,
		OriginSiteName:       TestData.Site,
	}
	blockParentDetailResp := blockDetailResponse{
		BlockID:              2,
		BlockName:            TestData.ParentBlock,
		BlockSize:            0,
		CreateBy:             TestData.CreateBy,
		CreationDate:         0,
		Dataset:              TestData.ParentDataset,
		DatasetID:            2,
		FileCount:            0,
		LastModificationDate: 0,
		LastModifiedBy:       TestData.CreateBy,
		OpenForWriting:       0,
		OriginSiteName:       TestData.Site,
	}
	return EndpointTestCase{
		description:     "Test blocks",
		defaultHandler:  web.BlocksHandler,
		defaultEndpoint: "/dbs/blocks",
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
				input:       blockReq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					blockResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test parent POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       parentBlockReq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test duplicate POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       blockReq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					blockResp,
					blockParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test detailed GET",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"detail": []string{"true"},
				},
				output: []Response{
					blockDetailResp,
					blockParentDetailResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}
