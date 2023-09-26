package main

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// response for fileparent
type fileParentResponse struct {
	LOGICAL_FILE_NAME        string `json:"logical_file_name"`
	PARENT_FILE_ID           int    `json:"parent_file_id"`
	PARENT_LOGICAL_FILE_NAME string `json:"parent_logical_file_name"`
}

func getFileParentsTestTable(t *testing.T) EndpointTestCase {
	fpResp := fileParentResponse{
		LOGICAL_FILE_NAME:        TestData.Files[0],
		PARENT_FILE_ID:           1,
		PARENT_LOGICAL_FILE_NAME: TestData.ParentFiles[0],
	}
	dbsError := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Message:  "logical_file_name, block_id or block_name is required for fileparents api",
		Code:     dbs.ParametersErrorCode,
		Function: "dbs.fileparents.FileParents",
	}
	hrec := createHTTPError("GET", "/dbs/primarydstypes?dataset=fnal")
	errorResp := web.ServerError{
		HTTPError: hrec,
		DBSError:  &dbsError,
		Exception: http.StatusBadRequest,
		Type:      "HTTPError",
		Message:   dbsError.Error(),
	}
	return EndpointTestCase{
		description:     "Test fileparents",
		defaultHandler:  web.FileParentsHandler,
		defaultEndpoint: "/dbs/fileparents",
		testCases: []testCase{
			{ // DBSClientReader_t.test041
				description: "Test fileparents with parent lfn",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{TestData.ParentFiles[0]},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{ // DBSClientReader_t.test041
				description: "Test fileparents with file lfn",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{TestData.Files[0]},
				},
				output:   []Response{fpResp},
				respCode: http.StatusOK,
			},
			{ // DBSClientReader_t.test042
				description: "Test fileparents with no params",
				method:      "GET",
				serverType:  "DBSReader",
				params:      url.Values{},
				output:      []Response{errorResp},
				respCode:    http.StatusBadRequest,
			},
		},
	}
}
