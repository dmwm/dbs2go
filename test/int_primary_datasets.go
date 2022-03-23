package main

// this file contains logic for primarydatasets and primarydstypes API
//
// primarydatasets:
// the HTTP request body is defined by dbs.PrimaryDatasetRecord struct defined in dbs/primarydatasets.go
// the HTTP response body is defined by dbs.PrimaryDataset struct defined in this dbs/primarydatasets.go
//
// primarydstypes:
// the HTTP response body is defined by primaryDSTypesReponse struct defined in this dbs/primarydstypes.go
//
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// expected response from primarydstypes GET API
type primaryDSTypesResponse struct {
	DATA_TYPE          string `json:"data_type"`
	PRIMARY_DS_TYPE_ID int64  `json:"primary_ds_type_id"`
}

// primarydataset and primarydstype endpoints tests
func getPrimaryDatasetTestTable(t *testing.T) EndpointTestCase {
	// create data structs for expected requests and responses
	primaryDSReq := dbs.PrimaryDatasetRecord{
		PRIMARY_DS_NAME: TestData.PrimaryDSName,
		PRIMARY_DS_TYPE: "test",
		CREATE_BY:       "tester",
	}
	primaryDSResp := dbs.PrimaryDataset{
		PrimaryDSId:   1.0,
		PrimaryDSType: "test",
		PrimaryDSName: TestData.PrimaryDSName,
		CreationDate:  0,
		CreateBy:      "tester",
	}
	primaryDSTypeResp := primaryDSTypesResponse{
		PRIMARY_DS_TYPE_ID: 1.0,
		DATA_TYPE:          "test",
	}
	primaryDSReq2 := dbs.PrimaryDatasetRecord{
		PRIMARY_DS_NAME: TestData.PrimaryDSName2,
		PRIMARY_DS_TYPE: "test",
		CREATE_BY:       "tester",
	}
	primaryDSResp2 := dbs.PrimaryDataset{
		PrimaryDSId:   2.0,
		PrimaryDSType: "test",
		PrimaryDSName: TestData.PrimaryDSName2,
		CreateBy:      "tester",
		CreationDate:  0,
	}
	dbsError1 := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Message:  "unable to match 'dataset' value 'fnal'",
		Code:     dbs.PatternErrorCode,
		Function: "dbs.validator.Check",
	}
	dbsError := dbs.DBSError{
		Function: "dbs.Validate",
		Code:     dbs.ValidateErrorCode,
		Reason:   dbsError1.Error(),
		Message:  "not str type",
	}
	hrec := web.HTTPError{
		Method:    "GET",
		Timestamp: "",
		HTTPCode:  http.StatusBadRequest,
		Path:      "/dbs/primarydstypes?dataset=fnal",
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
		description:     "Test primarydataset",
		defaultHandler:  web.PrimaryDatasetsHandler,
		defaultEndpoint: "/dbs/primarydatasets",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				serverType:  "DBSReader",
				method:      "GET",
				params:      nil,
				input:       nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydstypes GET with no Data",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				params:      nil,
				handler:     web.PrimaryDSTypesHandler,
				input:       nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets bad POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: BadRequest{
					BAD_FIELD: "Bad",
				},
				params:   nil,
				output:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test primarydatasets POST",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input:       primaryDSReq,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResp,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydstypes GET",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params:   nil,
				respCode: http.StatusOK,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydstypes GET w primary_ds_type param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params: url.Values{
					"primary_ds_type": []string{"test"},
				},
				respCode: http.StatusOK,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydstypes GET w primary_ds_type wildcard param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params: url.Values{
					"primary_ds_type": []string{"t*"},
				},
				handler:  web.PrimaryDSTypesHandler,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydstypes GET w dataset param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params: url.Values{
					"dataset": []string{"unittest"},
				},
				handler:  web.PrimaryDSTypesHandler,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydstypes GET w bad dataset param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					errorResp,
				},
				params: url.Values{
					"dataset": []string{"fnal"},
				},
				respCode: http.StatusBadRequest,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydstypes GET w different params",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output:      []Response{},
				params: url.Values{
					"primary_ds_type": []string{"A*"},
				},
				respCode: http.StatusOK,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydataset POST duplicate",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input:       primaryDSReq,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after duplicate POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydataset second POST",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input:       primaryDSReq2,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after second POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResp,
					primaryDSResp2,
				},
				respCode: http.StatusOK,
			},
		},
	}
}
