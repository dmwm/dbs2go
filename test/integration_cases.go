package main

import (
	"net/http"
	"net/url"

	"github.com/vkuznet/dbs2go/web"
)

type Response map[string]interface{}

type RequestBody map[string]interface{}

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
	description     string
	defaultHandler  func(http.ResponseWriter, *http.Request)
	defaultEndpoint string
	testCases       []testCase
}

// PrimaryDatasetTestCase contains test cases for primarydataset and primarydstype endpoints
var PrimaryDatasetAndTypesTestCase = EndpointTestCase{
	description:     "Test primarydataset",
	defaultHandler:  web.PrimaryDatasetsHandler,
	defaultEndpoint: "/dbs/primarydatasets",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			serverType:  "DBSReader",
			method:      "GET",
			params:      nil,
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
			params:      nil,
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
			resp: []Response{
				{
					"primary_ds_id":   1.0,
					"primary_ds_name": "unittest",
					"creation_date":   0,
					"create_by":       "tester",
					"primary_ds_type": "test",
				},
			},
			params:   nil,
			respCode: http.StatusOK,
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
			handler:  web.PrimaryDSTypesHandler,
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
			handler:  web.PrimaryDSTypesHandler,
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
			handler:  web.PrimaryDSTypesHandler,
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
			handler:  web.PrimaryDSTypesHandler,
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
			handler:  web.PrimaryDSTypesHandler,
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
			handler:  web.PrimaryDSTypesHandler,
		},
		{
			description: "Test primarydataset POST duplicate",
			method:      "POST",
			serverType:  "DBSWriter",
			params:      nil,
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
		},
		{
			description: "Test primarydataset second POST",
			method:      "POST",
			serverType:  "DBSWriter",
			params:      nil,
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
		},
	},
}

// OutputConfigTestCase contains tests for outputconfigs endpoint
// TODO: Rest of test cases
var OutputConfigTestCase = EndpointTestCase{
	description:     "Test outputconfigs",
	defaultHandler:  web.OutputConfigsHandler,
	defaultEndpoint: "/dbs/outputconfigs",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			method:      "GET",
			serverType:  "DBSReader",
			record:      nil,
			params:      nil,
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test bad POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"bad-field": "Bad",
			},
			params:   nil,
			resp:     nil,
			respCode: http.StatusBadRequest,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"app_name":            "cmsRun",
				"release_version":     "CMSSW_1_2_3",
				"pset_hash":           "76e303993a1c2f842159dbfeeed9a0dd",
				"global_tag":          "my-cms-gtag",
				"output_module_label": "Merged",
				"create_by":           "tester",
				"scenario":            "note",
			},
			resp:     nil,
			params:   nil,
			respCode: http.StatusOK,
		},
		{
			description: "Test duplicate POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"app_name":            "cmsRun",
				"release_version":     "CMSSW_1_2_3",
				"pset_hash":           "76e303993a1c2f842159dbfeeed9a0dd",
				"global_tag":          "my-cms-gtag",
				"output_module_label": "Merged",
				"create_by":           "tester",
				"scenario":            "note",
			},
			resp:     nil,
			params:   nil,
			respCode: http.StatusOK,
		},
		{
			description: "Test GET after POST",
			method:      "GET",
			serverType:  "DBSReader",
			resp: []Response{
				{
					"app_name":            "cmsRun",
					"release_version":     "CMSSW_1_2_3",
					"pset_hash":           "76e303993a1c2f842159dbfeeed9a0dd",
					"global_tag":          "my-cms-gtag",
					"output_module_label": "Merged",
					"create_by":           "tester",
					"creation_date":       0,
				},
			},
			params:   nil,
			respCode: http.StatusOK,
		},
	},
}

// AcquisitioneraTestCase tests the acquisitioneras endpoint
// TODO: Rest of test cases
var AcquisitionEraTestCase = EndpointTestCase{
	description:     "Test acquisitioneras",
	defaultHandler:  web.AcquisitionErasHandler,
	defaultEndpoint: "/dbs/acquisitioneras",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			method:      "GET",
			serverType:  "DBSReader",
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"acquisition_era_name": "acq_era",
				"create_by":            "tester",
				"description":          "note",
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test GET after POST",
			method:      "GET",
			serverType:  "DBSReader",
			resp: []Response{
				{
					"acquisition_era_name": "acq_era",
					"start_date":           0,
					"end_date":             0,
					"creation_date":        0,
					"create_by":            "tester",
					"description":          "note",
				},
			},
			respCode: http.StatusOK,
		},
	},
}

// ProcessingEraTestCase test the processingeras endpoint
// TODO: Rest of test cases
var ProcessingEraTestCase = EndpointTestCase{
	description:     "Test processingeras",
	defaultHandler:  web.ProcessingErasHandler,
	defaultEndpoint: "/dbs/processingeras",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			method:      "GET",
			serverType:  "DBSReader",
			record:      nil,
			params:      nil,
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"processing_version": 7269,
				"description":        "this_is_a_test",
				"create_by":          "tester",
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test GET after POST",
			method:      "GET",
			serverType:  "DBSReader",
			resp: []Response{
				{
					"processing_version": 7269,
					"description":        "this_is_a_test",
					"create_by":          "tester",
					"creation_date":      0,
				},
			},
			respCode: http.StatusOK,
		},
	},
}

// DatatiersTestCase contains tests for the datatiers endpoint
var DatatiersTestCase = EndpointTestCase{
	description:     "Test datatiers",
	defaultHandler:  web.DatatiersHandler,
	defaultEndpoint: "/dbs/datatiers",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			method:      "GET",
			serverType:  "DBSReader",
			record:      nil,
			params:      nil,
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test bad POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"non-existing-field": "GEN-SIM-RAW",
			},
			params:   nil,
			respCode: http.StatusBadRequest,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
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
		},
		{
			description: "Test GET after POST",
			method:      "GET",
			serverType:  "DBSReader",
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
		},
		{
			description: "Test GET with parameters",
			method:      "GET",
			serverType:  "DBSReader",
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
		},
		{
			description: "Test GET with regex parameter",
			method:      "GET",
			serverType:  "DBSReader",
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
		},
		{
			description: "Test GET with non-existing parameter value",
			method:      "GET",
			serverType:  "DBSReader",
			record: RequestBody{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			},
			resp: []Response{},
			params: url.Values{
				"data_tier_name": []string{"A*"},
			},
			respCode: http.StatusOK,
		},
	},
}

// DatasetAccessTypesTestCase tests the datasetaccesstypes endpoint
var DatasetAccessTypesTestCase = EndpointTestCase{
	description:     "Test datasetaccesstypes",
	defaultHandler:  web.DatasetAccessTypesHandler,
	defaultEndpoint: "/dbs/datasetaccesstypes",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			method:      "GET",
			serverType:  "DBSReader",
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"dataset_access_type": "PRODUCTION",
			},
			resp:     []Response{},
			respCode: http.StatusOK,
		},
		{
			description: "Test GET after POST",
			method:      "GET",
			serverType:  "DBSReader",
			resp: []Response{
				{
					"dataset_access_type": "PRODUCTION",
				},
			},
			respCode: http.StatusOK,
		},
	},
}

// PhysicsGroupsTestCase tests for the physicsgroups endpoint
var PhysicsGroupsTestCase = EndpointTestCase{
	description:     "Test physicsgroups",
	defaultHandler:  web.PhysicsGroupsHandler,
	defaultEndpoint: "/dbs/physicsgroups",
	testCases: []testCase{
		{
			description: "Test GET with no data",
			serverType:  "DBSReader",
			method:      "GET",
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test POST",
			serverType:  "DBSWriter",
			method:      "POST",
			record: RequestBody{
				"physics_group_name": "Tracker",
			},
			respCode: http.StatusOK,
		},
		{
			description: "Test GET after POST",
			serverType:  "DBSReader",
			method:      "GET",
			resp: []Response{
				{
					"physics_group_name": "Tracker",
				},
			},
		},
	},
}

// DatasetsTestCase contains tests for the datasets endpoint
//* Note: depends on above tests for their *_id
// TODO: include prep_id in POST tests
var DatasetsTestCase = EndpointTestCase{
	description:     "Test datasets",
	defaultHandler:  web.DatasetsHandler,
	defaultEndpoint: "/dbs/datasets",
	testCases: []testCase{
		{
			description: "Test empty GET",
			method:      "GET",
			serverType:  "DBSReader",
			resp:        []Response{},
			respCode:    http.StatusOK,
		},
		{
			description: "Test POST",
			method:      "POST",
			serverType:  "DBSWriter",
			record: RequestBody{
				"physics_group_name":  "Tracker",
				"dataset":             "unittest",
				"dataset_access_type": "PRODUCTION",
				"processed_ds_name":   "acq_era",
				"primary_ds_name":     "unittest",
				"output_configs": []RequestBody{
					{
						"release_version":     "CMSSW_1_2_3",
						"pset_hash":           "76e303993a1c2f842159dbfeeed9a0dd",
						"app_name":            "cmsRun",
						"output_module_label": "Merged",
						"global_tag":          "my-cms-gtag",
					},
				},
				"xtcrosssection":         123,
				"primary_ds_type":        "test",
				"data_tier_name":         "GEN-SIM-RAW",
				"creation_date":          1635177605,
				"create_by":              "tester2",
				"last_modification_date": 1635177605,
				"last_modified_by":       "tester3",
				"processing_version":     7269,
				"acquisition_era_name":   "acq_era",
			},
			resp:     []Response{},
			respCode: http.StatusOK,
		},
	},
}

var IntegrationTestCases = []EndpointTestCase{
	PrimaryDatasetAndTypesTestCase,
	OutputConfigTestCase,
	AcquisitionEraTestCase,
	ProcessingEraTestCase,
	DatatiersTestCase,
	DatasetAccessTypesTestCase,
	PhysicsGroupsTestCase,
	DatasetsTestCase,
}
