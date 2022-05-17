package main

// this file contains logic for the blocks API
// the HTTP requests body is defined by dbs.Blocks struct defined in dbs/blocks.go
// the HTTP response body is defined by blockResponse struct defined in this file
// the HTTP response body for the `detail` query is defined by blockDetailResponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
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
	dbsError := dbs.DBSError{
		Function: "dbs.blocks.Blocks",
		Code:     dbs.ParametersErrorCode,
		Reason:   dbs.InvalidParamErr.Error(),
		Message:  "Blocks API requires one of the following: [dataset block_name data_tier_name logical_file_name]",
	}
	hrec := web.HTTPError{
		Method:    "GET",
		Timestamp: "",
		HTTPCode:  http.StatusBadRequest,
		Path:      "/dbs/blocks?origin_site_name=cmssrm.fnal.gov",
		UserAgent: "Go-http-client/1.1",
	}
	errorResp := web.ServerError{
		HTTPError: hrec,
		DBSError:  &dbsError,
		Exception: http.StatusBadRequest,
		Type:      "HTTPError",
		Message:   dbsError.Error(),
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
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       blockReq,
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
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
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test duplicate POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       blockReq,
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset, TestData.ParentDataset},
				},
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
					"dataset": []string{TestData.Dataset, TestData.ParentDataset},
					"detail":  []string{"true"},
				},
				output: []Response{
					blockDetailResp,
					blockParentDetailResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block", // DBSClientReader_t.test025
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.Block},
				},
				output: []Response{
					blockResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing block",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.Block + "1"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset", // DBSClientReader_t.test026
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output: []Response{
					blockResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing dataset",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset + "1"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block and origin_site_name", // DBSClientReader_t.test027
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":       []string{TestData.Block},
					"origin_site_name": []string{TestData.Site},
				},
				output: []Response{
					blockResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset and origin_site_name", // DBSClientReader_t.test028
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":          []string{TestData.Dataset},
					"origin_site_name": []string{TestData.Site},
				},
				output: []Response{
					blockResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset block and origin_site_name", // DBSClientReader_t.test029a
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":          []string{TestData.Dataset},
					"block_name":       []string{TestData.Block},
					"origin_site_name": []string{TestData.Site},
				},
				output: []Response{
					blockResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with only origin_site_name", // DBSClientReader_t.test029a1
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"origin_site_name": []string{TestData.Site},
				},
				output: []Response{
					errorResp,
				},
				respCode: http.StatusBadRequest,
			},
		},
	}
}

// struct for block status update
type blockUpdateStatusRequest struct {
	BLOCK_NAME       string `json:"block_name"`
	OPEN_FOR_WRITING string `json:"open_for_writing"`
}

// struct for block status update
type blockUpdateSiteRequest struct {
	BLOCK_NAME       string `json:"block_name"`
	ORIGIN_SITE_NAME string `json:"origin_site_name"`
}

// detailed blocks API response
type blockRunDetailResponse struct {
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
	RunNum               int64  `json:"run_num"`
}

// create a detailed response with run_num
func createBlockRunDetailResponse(blockID int64, runNum int) blockRunDetailResponse {
	return blockRunDetailResponse{
		BlockID:              blockID,
		BlockName:            TestData.Block,
		BlockSize:            20122119010,
		CreateBy:             TestData.CreateBy,
		CreationDate:         0,
		Dataset:              TestData.Dataset,
		DatasetID:            1,
		FileCount:            10,
		LastModificationDate: 0,
		LastModifiedBy:       TestData.CreateBy,
		OpenForWriting:       0,
		OriginSiteName:       TestData.Site,
		RunNum:               int64(runNum),
	}
}

// detailed blocks API response
func getBlocksTestTable2(t *testing.T) EndpointTestCase {
	blkStatusReq := blockUpdateStatusRequest{
		BLOCK_NAME:       TestData.Block,
		OPEN_FOR_WRITING: "1",
	}
	blkSiteReq := blockUpdateSiteRequest{
		BLOCK_NAME:       TestData.Block,
		ORIGIN_SITE_NAME: "cmssrm2.fnal.gov",
	}
	blockDetailResp := blockDetailResponse{
		BlockID:              1,
		BlockName:            TestData.Block,
		BlockSize:            20122119010,
		CreateBy:             TestData.CreateBy,
		CreationDate:         0,
		Dataset:              TestData.Dataset,
		DatasetID:            1,
		FileCount:            10,
		LastModificationDate: 0,
		LastModifiedBy:       TestData.CreateBy,
		OpenForWriting:       0,
		OriginSiteName:       TestData.Site,
	}
	blockDetailResp2 := blockDetailResp
	blockDetailResp2.OpenForWriting = 1
	blockDetailResp2.LastModifiedBy = "DBS-workflow"
	blockDetailResp3 := blockDetailResp2
	blockDetailResp3.OriginSiteName = "cmssrm2.fnal.gov"

	runs := strings.ReplaceAll(fmt.Sprint(TestData.Runs), " ", ",")
	var blockRunDetailResp []Response
	for _, run := range TestData.Runs {
		blockRunDetailResp = append(blockRunDetailResp, createBlockRunDetailResponse(1, run))
	}
	return EndpointTestCase{
		description:     "Test blocks update API",
		defaultEndpoint: "/dbs/blocks",
		defaultHandler:  web.BlocksHandler,
		testCases: []testCase{
			{
				description: "Test GET with dataset, run_num, detail", // DBSClientReader_t.test029b
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
					"run_num": []string{runs},
					"detail":  []string{"true"},
				},
				output:   blockRunDetailResp,
				respCode: http.StatusOK,
			},
			{
				description: "Initial GET block",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"block_name": []string{TestData.Block},
					"detail":     []string{"true"},
				},
				output:   []Response{blockDetailResp},
				respCode: http.StatusOK,
			},
			{
				description: "Test update block status", // DBSClientWriter_t.test21
				serverType:  "DBSWriter",
				method:      "PUT",
				params: url.Values{
					"block_name":       []string{TestData.Block},
					"open_for_writing": []string{"1"},
				},
				input:    blkStatusReq,
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "GET block after status update",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"block_name": []string{TestData.Block},
					"detail":     []string{"true"},
				},
				output:   []Response{blockDetailResp2},
				respCode: http.StatusOK,
			},
			{
				description: "Test update block site name", // DBSClientWriter_t.test22
				serverType:  "DBSWriter",
				method:      "PUT",
				params: url.Values{
					"block_name":       []string{TestData.Block},
					"origin_site_name": []string{"cmssrm2.fnal.gov"},
				},
				input:    blkSiteReq,
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "GET block after site update",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"block_name": []string{TestData.Block},
					"detail":     []string{"true"},
				},
				output:   []Response{blockDetailResp3},
				respCode: http.StatusOK,
			},
		},
	}
}
