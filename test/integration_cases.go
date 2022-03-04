package main

import (
	"net/http"
	"net/url"

	"github.com/vkuznet/dbs2go/web"
)

type Response map[string]interface{}

type RequestBody map[string]string

type testFields []string

// basic elements to define a test case
type testCase struct {
	description string      // test case description
	method      string      // http method
	endpoint    string      // url endpoint
	params      url.Values  // url parameters
	record      RequestBody // POST record
	resp        []Response  // expected response
	fields      testFields  // expected fields
	respCode    int         // expected HTTP response code
	handler     func(http.ResponseWriter, *http.Request)
}

// defines a testcase for an endpoint
type EndpointTestCase struct {
	description string
	testCases   []testCase
}

// PrimaryDatasetTestCase contains test cases for primarydataset and primarydstype endpoints
// TODO: test dataset parameter
var PrimaryDatasetAndTypesTestCase = EndpointTestCase{
	description: "Test primarydataset",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			method:      "GET",
			endpoint:    "/dbs/primarydatasets",
			params:      nil,
			resp:        []Response{},
			fields:      nil,
			handler:     web.PrimaryDatasetsHandler,
			respCode:    http.StatusOK,
		},
		{
			description: "Test primarydstypes GET with no Data",
			method:      "GET",
			endpoint:    "/dbs/primarydstypes",
			params:      nil,
			resp:        []Response{},
			fields:      nil,
			handler:     web.PrimaryDSTypesHandler,
			respCode:    http.StatusOK,
		},
		{
			description: "Test POST",
			method:      "POST",
			endpoint:    "/dbs/primarydatasets",
			record: RequestBody{
				"primary_ds_name": "unittest",
				"primary_ds_type": "test",
				"create_by":       "tester",
			},
			params:   nil,
			fields:   nil,
			respCode: http.StatusOK,
			handler:  web.PrimaryDatasetsHandler,
		},
		{
			description: "Test primarydatasets GET after POST",
			method:      "GET",
			endpoint:    "/dbs/primarydatasets",
			resp: []Response{
				{
					"primary_ds_id":   1.0,
					"primary_ds_name": "unittest",
					"creation_date":   0,
					"create_by":       "tester",
					"primary_ds_type": "test",
				},
			},
			fields: testFields{
				"primary_ds_id",
				"primary_ds_name",
				"creation_date",
				"create_by",
				"primary_ds_type",
			},
			respCode: http.StatusOK,
			handler:  web.PrimaryDatasetsHandler,
		},
		{
			description: "Test primarydstypes GET",
			method:      "GET",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp: []Response{
				{
					"primary_ds_type_id": 1.0,
					"data_type":          "test",
				},
			},
			params: nil,
			fields: testFields{
				"primary_ds_type_id",
				"data_type",
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w primary_ds_type param",
			method:      "GET",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp: []Response{
				{
					"primary_ds_type_id": 1.0,
					"data_type":          "test",
				},
			},
			params: url.Values{
				"primary_ds_type": []string{"test"},
			},
			fields: testFields{
				"primary_ds_type_id",
				"data_type",
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w primary_ds_type param regex",
			method:      "GET",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp: []Response{
				{
					"primary_ds_type_id": 1.0,
					"data_type":          "test",
				},
			},
			params: url.Values{
				"primary_ds_type": []string{"t*"},
			},
			fields: testFields{
				"primary_ds_type_id",
				"data_type",
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w different params",
			method:      "GET",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp:        []Response{},
			params: url.Values{
				"primary_ds_type": []string{"A*"},
			},
			fields: testFields{
				"primary_ds_type_id",
				"data_type",
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w dataset param",
			method:      "GET",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp: []Response{
				{
					"primary_ds_type_id": 1.0,
					"data_type":          "test",
				},
			},
			params: url.Values{
				"dataset": []string{"banana"},
			},
			fields: testFields{
				"primary_ds_type_id",
				"data_type",
			},
			respCode: http.StatusBadRequest,
		},
	},
}

// DatatiersTestCase contains tests for the datatiers endpoint
var DatatiersTestCase = EndpointTestCase{
	description: "Test datatiers",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			method:      "GET",
			endpoint:    "/dbs/datatiers",
			record:      nil,
			params:      nil,
			fields: testFields{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			},
			resp:     []Response{},
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test bad POST",
			method:      "POST",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"non-existing-field": "GEN-SIM-RAW",
			},
			params: nil,
			fields: testFields{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			},
			respCode: http.StatusBadRequest,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test POST",
			method:      "POST",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			},
			resp: []Response{
				{
					"data_tier_id":   "1",
					"data_tier_name": "GEN-SIM-RAW",
					"create_by":      "tester",
				},
			},
			params: nil,
			fields: testFields{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			},
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET after POST",
			method:      "GET",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			},
			resp: []Response{
				{
					"data_tier_name": "GEN-SIM-RAW",
					"create_by":      "tester",
					"creation_date":  "0",
					"data_tier_id":   1.0,
				},
			},
			params: nil,
			fields: testFields{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			},
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET with parameters",
			method:      "GET",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			},
			resp: []Response{
				{
					"data_tier_name": "GEN-SIM-RAW",
					"create_by":      "tester",
					"creation_date":  "0",
					"data_tier_id":   1.0,
				},
			},
			params: url.Values{
				"data_tier_name": []string{"GEN-SIM-RAW"},
			},
			fields: testFields{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			},
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET with regex parameter",
			method:      "GET",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			},
			resp: []Response{
				{
					"data_tier_name": "GEN-SIM-RAW",
					"create_by":      "tester",
					"creation_date":  "0",
					"data_tier_id":   1.0,
				},
			},
			params: url.Values{
				"data_tier_name": []string{"G*"},
			},
			fields: testFields{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			},
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET with non-existing parameter value",
			method:      "GET",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			},
			resp: []Response{},
			params: url.Values{
				"data_tier_name": []string{"A*"},
			},
			fields: testFields{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			},
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
	},
}

var IntegrationTestCases = []EndpointTestCase{
	PrimaryDatasetAndTypesTestCase,
	// DatatiersTestCase,
}
