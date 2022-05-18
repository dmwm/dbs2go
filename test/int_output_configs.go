package main

// this file contains logic for outputconfigs API
// the HTTP request body is defined by dbs.OutputConfigRecord struct defined in dbs/outputconfigs.go
// the HTTP response body is defined by outputConfigResponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// expected response from outputconfigs GET API
type outputConfigResponse struct {
	APP_NAME            string `json:"app_name"`
	RELEASE_VERSION     string `json:"release_version"`
	PSET_HASH           string `json:"pset_hash"`
	PSET_NAME           string `json:"pset_name"`
	GLOBAL_TAG          string `json:"global_tag"`
	OUTPUT_MODULE_LABEL string `json:"output_module_label"`
	CREATION_DATE       int64  `json:"creation_date"`
	CREATE_BY           string `json:"create_by"`
}

func createOutputConfigResponse(t *testing.T) outputConfigResponse {
	return outputConfigResponse{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           TestData.CreateBy,
		CREATION_DATE:       0,
	}
}

// outputconfigs endpoint tests
// TODO: Rest of test cases
func getOutputConfigTestTable(t *testing.T) EndpointTestCase {
	outputConfigReq := dbs.OutputConfigRecord{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           TestData.CreateBy,
		SCENARIO:            "note",
	}
	outputConfigResp := createOutputConfigResponse(t)

	ocError := dbs.CreateInvalidParamError("fnal", "outputconfigs")
	hrec := createHTTPError("GET", "/dbs/outputconfigs?fnal=cern")
	errorResp := createServerErrorResponse(hrec, ocError)
	return EndpointTestCase{
		description:     "Test outputconfigs",
		defaultHandler:  web.OutputConfigsHandler,
		defaultEndpoint: "/dbs/outputconfigs",
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
				description: "Test bad POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: BadRequest{
					BAD_FIELD: "bad",
				},
				params:   nil,
				output:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test POST", // DBSClientWriter_t.test04
				method:      "POST",
				serverType:  "DBSWriter",
				input:       outputConfigReq,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test duplicate POST", // DBSClientWriter_t.test05
				method:      "POST",
				serverType:  "DBSWriter",
				input:       outputConfigReq,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST", // DBSClientReader_t.test017
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					outputConfigResp,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with invalid parameter key",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"fnal": []string{"cern"},
				},
				output: []Response{
					errorResp,
				},
				respCode: http.StatusBadRequest,
			},
		},
	}
}

// outputconfigs endpoint test 2
func getOutputConfigTestTable2(t *testing.T) EndpointTestCase {
	outputConfigResp := createOutputConfigResponse(t)
	splitLFN := strings.Split(TestData.Files[0], ".")
	badLFN := splitLFN[0] + "abc" + splitLFN[1]
	return EndpointTestCase{
		description:     "Test outputconfigs 2",
		defaultHandler:  web.OutputConfigsHandler,
		defaultEndpoint: "/dbs/outputconfigs",
		testCases: []testCase{
			{
				description: "Test GET using dataset", // DBSClientReader_t.test015
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing dataset",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset + "abc"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET using logical_file_name", // DBSClientReader_t.test016
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{TestData.Files[0]},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET using non-existing logical_file_name",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{badLFN},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET all", // DBSClientReader_t.test017
				method:      "GET",
				serverType:  "DBSReader",
				params:      url.Values{},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with release_version", // DBSClientReader_t.test018
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"release_version": []string{TestData.ReleaseVersion},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing release_version",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"release_version": []string{TestData.ReleaseVersion + "3"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with pset_hash", // DBSClientReader_t.test019
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"pset_hash": []string{TestData.PsetHash},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing pset_hash",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"pset_hash": []string{TestData.PsetHash + "a"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with app_name", // DBSClientReader_t.test020
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"app_name": []string{TestData.AppName},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing app_name",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"app_name": []string{TestData.AppName + "a"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with output_module_label", // DBSClientReader_t.test021
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing output_module_label",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"output_module_label": []string{TestData.OutputModuleLabel + "a"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with output_mod fields", // DBSClientReader_t.test022
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"release_version":     []string{TestData.ReleaseVersion},
					"pset_hash":           []string{TestData.PsetHash},
					"app_name":            []string{TestData.AppName},
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset and output_mod fields", // DBSClientReader_t.test023
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":             []string{TestData.Dataset},
					"release_version":     []string{TestData.ReleaseVersion},
					"pset_hash":           []string{TestData.PsetHash},
					"app_name":            []string{TestData.AppName},
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset and release_version", // DBSClientReader_t.test024
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":         []string{TestData.Dataset},
					"release_version": []string{TestData.ReleaseVersion},
				},
				output: []Response{
					outputConfigResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}
