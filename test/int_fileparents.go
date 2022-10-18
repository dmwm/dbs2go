package main

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

func getFileParentsTestTable(t *testing.T) EndpointTestCase {
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
				description: "Test fileparents with lfn",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{TestData.ParentFiles[0]},
				},
				output:   []Response{},
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
