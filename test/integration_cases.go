package main

import (
	"net/http"
	"net/url"

	"github.com/vkuznet/dbs2go/web"
)

type Response map[string]interface{}

type RequestBody map[string]string

// type testFields []string

// basic elements to define a test case
type testCase struct {
	description string     // test case description
	serverType  string     //DBSWriter, DBSReader, DBSMigrate
	method      string     // http method
	endpoint    string     // url endpoint
	params      url.Values // url parameters
	handler     func(http.ResponseWriter, *http.Request)
	record      RequestBody // POST record
	resp        []Response  // expected response
	// fields      testFields  // expected fields
	respCode int // expected HTTP response code
}

// defines a testcase for an endpoint
type EndpointTestCase struct {
	description string
	testCases   []testCase
}

// PrimaryDatasetTestCase contains test cases for primarydataset and primarydstype endpoints
var PrimaryDatasetAndTypesTestCase = EndpointTestCase{
	description: "Test primarydataset",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			serverType:  "DBSReader",
			method:      "GET",
			endpoint:    "/dbs/primarydatasets",
			params:      nil,
			handler:     web.PrimaryDatasetsHandler,
			record:      nil,
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test primarydstypes GET with no Data",
			method:      "GET",
			serverType:  "DBSReader",
			endpoint:    "/dbs/primarydstypes",
			params:      nil,
			handler:     web.PrimaryDSTypesHandler,
			record:      nil,
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
			endpoint:    "/dbs/primarydatasets",
			params:      nil,
			handler:     web.PrimaryDatasetsHandler,
			record: RequestBody{
				"primary_ds_name": "unittest",
				"primary_ds_type": "test",
				"create_by":       "tester",
			},
			resp:     nil,
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydatasets GET after POST",
			method:      "GET",
			serverType:  "DBSReader",
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
			respCode: http.StatusOK,
			handler:  web.PrimaryDatasetsHandler,
		},
		{
			description: "Test primarydstypes GET",
			method:      "GET",
			serverType:  "DBSReader",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp: []Response{
				{
					"primary_ds_type_id": 1.0,
					"data_type":          "test",
				},
			},
			params:   nil,
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w primary_ds_type param",
			method:      "GET",
			serverType:  "DBSReader",
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
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w primary_ds_type wildcard param",
			method:      "GET",
			serverType:  "DBSReader",
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
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w dataset param",
			method:      "GET",
			serverType:  "DBSReader",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp: []Response{
				{
					"primary_ds_type_id": 1.0,
					"data_type":          "test",
				},
			},
			params: url.Values{
				"dataset": []string{"unittest"},
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydstypes GET w bad dataset param",
			method:      "GET",
			serverType:  "DBSReader",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp: []Response{
				{
					"error": 113.0,
				},
			},
			params: url.Values{
				"dataset": []string{"fnal"},
			},
			respCode: http.StatusBadRequest,
		},
		{
			description: "Test primarydstypes GET w different params",
			method:      "GET",
			serverType:  "DBSReader",
			endpoint:    "/dbs/primarydstypes",
			record:      nil,
			resp:        []Response{},
			params: url.Values{
				"primary_ds_type": []string{"A*"},
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydataset POST duplicate",
			method:      "POST",
			serverType:  "DBSWriter",
			endpoint:    "/dbs/primarydatasets",
			params:      nil,
			handler:     web.PrimaryDatasetsHandler,
			record: RequestBody{
				"primary_ds_name": "unittest",
				"primary_ds_type": "test",
				"create_by":       "tester",
			},
			resp:     nil,
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydatasets GET after duplicate POST",
			method:      "GET",
			serverType:  "DBSReader",
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
			respCode: http.StatusOK,
			handler:  web.PrimaryDatasetsHandler,
		},
		{
			description: "Test primarydataset second POST",
			method:      "POST",
			serverType:  "DBSWriter",
			endpoint:    "/dbs/primarydatasets",
			params:      nil,
			handler:     web.PrimaryDatasetsHandler,
			record: RequestBody{
				"primary_ds_name": "unittest2",
				"primary_ds_type": "test",
				"create_by":       "tester",
			},
			resp:     nil,
			respCode: http.StatusOK,
		},
		{
			description: "Test primarydatasets GET after second POST",
			method:      "GET",
			serverType:  "DBSReader",
			endpoint:    "/dbs/primarydatasets",
			resp: []Response{
				{
					"primary_ds_id":   1.0,
					"primary_ds_name": "unittest",
					"creation_date":   0,
					"create_by":       "tester",
					"primary_ds_type": "test",
				},
				{
					"primary_ds_id":   2.0,
					"primary_ds_name": "unittest2",
					"creation_date":   0,
					"create_by":       "tester",
					"primary_ds_type": "test",
				},
			},
			respCode: http.StatusOK,
			handler:  web.PrimaryDatasetsHandler,
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
			serverType:  "DBSReader",
			endpoint:    "/dbs/datatiers",
			record:      nil,
			params:      nil,
			resp:        []Response{},
			respCode:    http.StatusOK,
			handler:     web.DatatiersHandler,
		},
		{
			description: "Test bad POST",
			method:      "POST",
			serverType:  "DBSWriter",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"non-existing-field": "GEN-SIM-RAW",
			},
			params:   nil,
			respCode: http.StatusBadRequest,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
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
			params:   nil,
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET after POST",
			method:      "GET",
			serverType:  "DBSReader",
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
			params:   nil,
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET with parameters",
			method:      "GET",
			serverType:  "DBSReader",
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
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET with regex parameter",
			method:      "GET",
			serverType:  "DBSReader",
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
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
		{
			description: "Test GET with non-existing parameter value",
			method:      "GET",
			serverType:  "DBSReader",
			endpoint:    "/dbs/datatiers",
			record: RequestBody{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			},
			resp: []Response{},
			params: url.Values{
				"data_tier_name": []string{"A*"},
			},
			respCode: http.StatusOK,
			handler:  web.DatatiersHandler,
		},
	},
}

var IntegrationTestCases = []EndpointTestCase{
	PrimaryDatasetAndTypesTestCase,
	DatatiersTestCase,
}
