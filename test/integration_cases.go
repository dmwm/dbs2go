package main

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/vkuznet/dbs2go/web"
)

// Response represents an expected HTTP response body
type Response map[string]interface{}

// RequestBody represents an expected HTTP request body
type RequestBody map[string]interface{}

// basic elements to define a test case
type testCase struct {
	description string     // test case description
	serverType  string     //DBSWriter, DBSReader, DBSMigrate
	method      string     // http method
	endpoint    string     // url endpoint
	params      url.Values // url parameters
	handler     func(http.ResponseWriter, *http.Request)
	input       RequestBody // POST record
	output      []Response  // expected response
	respCode    int         // expected HTTP response code
}

// defines a testcase for an endpoint
type EndpointTestCase struct {
	description     string
	defaultHandler  func(http.ResponseWriter, *http.Request)
	defaultEndpoint string
	testCases       []testCase
}

// LoadTestCases loads the testData from a json file
func LoadTestCases(t *testing.T, testData map[string]interface{}) []EndpointTestCase {
	var primaryDatasetAndTypesTestCase EndpointTestCase
	var outputConfigTestCase EndpointTestCase
	var acquisitionErasTestCase EndpointTestCase
	var processingErasTestCase EndpointTestCase
	var datatiersTestCase EndpointTestCase
	var datasetAccessTypesTestCase EndpointTestCase
	var physicsGroupsTestCase EndpointTestCase
	var datasetsTestCase EndpointTestCase

	// primarydataset and primarydstype endpoints tests
	primaryDatasetAndTypesTestCase = EndpointTestCase{
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
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input: RequestBody{
					"primary_ds_name": testData["primary_ds_name"],
					"primary_ds_type": "test",
					"create_by":       "tester",
				},
				output:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					{
						"primary_ds_id":   1.0,
						"primary_ds_name": testData["primary_ds_name"],
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
				input:       nil,
				output: []Response{
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
				input:       nil,
				output: []Response{
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
				input:       nil,
				output: []Response{
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
				input:       nil,
				output: []Response{
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
				input:       nil,
				output: []Response{
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
				input: RequestBody{
					"primary_ds_name": testData["primary_ds_name"],
					"primary_ds_type": "test",
					"create_by":       "tester",
				},
				output:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after duplicate POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					{
						"primary_ds_id":   1.0,
						"primary_ds_name": testData["primary_ds_name"],
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
				input: RequestBody{
					"primary_ds_name": "unittest2",
					"primary_ds_type": "test",
					"create_by":       "tester",
				},
				output:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after second POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					{
						"primary_ds_id":   1.0,
						"primary_ds_name": testData["primary_ds_name"],
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

	// outputconfigs endpoint tests
	// TODO: Rest of test cases
	outputConfigTestCase = EndpointTestCase{
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
				input: RequestBody{
					"bad-field": "Bad",
				},
				params:   nil,
				output:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"app_name":            testData["app_name"],
					"release_version":     testData["release_version"],
					"pset_hash":           testData["pset_hash"],
					"global_tag":          testData["global_tag"],
					"output_module_label": testData["output_module_label"],
					"create_by":           "tester",
					"scenario":            "note",
				},
				output:   nil,
				params:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test duplicate POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"app_name":            testData["app_name"],
					"release_version":     testData["release_version"],
					"pset_hash":           testData["pset_hash"],
					"global_tag":          testData["global_tag"],
					"output_module_label": testData["output_module_label"],
					"create_by":           "tester",
					"scenario":            "note",
				},
				output:   nil,
				params:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					{
						"app_name":            testData["app_name"],
						"release_version":     testData["release_version"],
						"pset_hash":           testData["pset_hash"],
						"global_tag":          testData["global_tag"],
						"output_module_label": testData["output_module_label"],
						"create_by":           "tester",
						"creation_date":       0,
					},
				},
				params:   nil,
				respCode: http.StatusOK,
			},
		},
	}

	// acquisitioneras endpoint tests
	// TODO: Rest of test cases
	acquisitionErasTestCase = EndpointTestCase{
		description:     "Test acquisitioneras",
		defaultHandler:  web.AcquisitionErasHandler,
		defaultEndpoint: "/dbs/acquisitioneras",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"acquisition_era_name": testData["acquisition_era"],
					"create_by":            "tester",
					"description":          "note",
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					{
						"acquisition_era_name": testData["acquisition_era"],
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

	// processingeras endpoint tests
	// TODO: Rest of test cases
	processingErasTestCase = EndpointTestCase{
		description:     "Test processingeras",
		defaultHandler:  web.ProcessingErasHandler,
		defaultEndpoint: "/dbs/processingeras",
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
				input: RequestBody{
					"processing_version": testData["processing_version"],
					"description":        "this_is_a_test",
					"create_by":          "tester",
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					{
						"processing_version": testData["processing_version"],
						"description":        "this_is_a_test",
						"create_by":          "tester",
						"creation_date":      0,
					},
				},
				respCode: http.StatusOK,
			},
		},
	}

	// datatiers endpoint tests
	datatiersTestCase = EndpointTestCase{
		description:     "Test datatiers",
		defaultHandler:  web.DatatiersHandler,
		defaultEndpoint: "/dbs/datatiers",
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
				input: RequestBody{
					"non-existing-field": testData["data_tier_name"],
				},
				params:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"data_tier_name": testData["data_tier_name"],
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_id":   "1",
						"data_tier_name": testData["data_tier_name"],
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
				input: RequestBody{
					"data_tier_name": testData["data_tier_name"],
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_name": testData["data_tier_name"],
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
				input: RequestBody{
					"data_tier_name": testData["data_tier_name"],
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_name": testData["data_tier_name"],
						"create_by":      "tester",
						"creation_date":  "0",
						"data_tier_id":   1.0,
					},
				},
				params: url.Values{
					"data_tier_name": []string{
						testData["data_tier_name"].(string),
					},
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with regex parameter",
				method:      "GET",
				serverType:  "DBSReader",
				input: RequestBody{
					"data_tier_name": testData["data_tier_name"],
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_name": testData["data_tier_name"],
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
				input: RequestBody{
					"data_tier_name": testData["data_tier_name"],
					"create_by":      "tester",
				},
				output: []Response{},
				params: url.Values{
					"data_tier_name": []string{"A*"},
				},
				respCode: http.StatusOK,
			},
		},
	}

	// datasetaccesstypes endpoint tests
	datasetAccessTypesTestCase = EndpointTestCase{
		description:     "Test datasetaccesstypes",
		defaultHandler:  web.DatasetAccessTypesHandler,
		defaultEndpoint: "/dbs/datasetaccesstypes",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"dataset_access_type": "PRODUCTION",
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					{
						"dataset_access_type": "PRODUCTION",
					},
				},
				respCode: http.StatusOK,
			},
		},
	}

	// contains tests for the physicsgroups endpoint
	physicsGroupsTestCase = EndpointTestCase{
		description:     "Test physicsgroups",
		defaultHandler:  web.PhysicsGroupsHandler,
		defaultEndpoint: "/dbs/physicsgroups",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				serverType:  "DBSReader",
				method:      "GET",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				serverType:  "DBSWriter",
				method:      "POST",
				input: RequestBody{
					"physics_group_name": "Tracker",
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				serverType:  "DBSReader",
				method:      "GET",
				output: []Response{
					{
						"physics_group_name": "Tracker",
					},
				},
				respCode: http.StatusOK,
			},
		},
	}

	// datasets endpoint tests
	//* Note: depends on above tests for their *_id
	// TODO: include prep_id in POST tests
	datasetsTestCase = EndpointTestCase{
		description:     "Test datasets",
		defaultHandler:  web.DatasetsHandler,
		defaultEndpoint: "/dbs/datasets",
		testCases: []testCase{
			{
				description: "Test empty GET",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"physics_group_name":  "Tracker",
					"dataset":             testData["dataset"],
					"dataset_access_type": "PRODUCTION",
					"processed_ds_name":   testData["processed_ds_name"],
					"primary_ds_name":     testData["primary_ds_name"],
					"output_configs": []RequestBody{
						{
							"release_version":     testData["release_version"],
							"pset_hash":           testData["pset_hash"],
							"app_name":            testData["app_name"],
							"output_module_label": testData["output_module_label"],
							"global_tag":          testData["global_tag"],
						},
					},
					"xtcrosssection":         123,
					"primary_ds_type":        "test",
					"data_tier_name":         testData["data_tier_name"],
					"creation_date":          1635177605,
					"create_by":              "tester",
					"last_modification_date": 1635177605,
					"last_modified_by":       "tester2",
					"processing_version":     testData["processing_version"],
					"acquisition_era_name":   testData["acquisition_era"],
				},
				output: []Response{
					{
						"dataset": "unittest",
					},
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test POST parent dataset",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"physics_group_name":  "Tracker",
					"dataset":             testData["dataset"],
					"dataset_access_type": "PRODUCTION",
					"processed_ds_name":   testData["processed_ds_name"],
					"primary_ds_name":     testData["primary_ds_name"],
					"output_configs": []RequestBody{
						{
							"release_version":     testData["release_version"],
							"pset_hash":           testData["pset_hash"],
							"app_name":            testData["app_name"],
							"output_module_label": testData["output_module_label"],
							"global_tag":          testData["global_tag"],
						},
					},
					"xtcrosssection":         123,
					"primary_ds_type":        "test",
					"data_tier_name":         testData["data_tier_name"],
					"creation_date":          1635177605,
					"create_by":              "tester",
					"last_modification_date": 1635177605,
					"last_modified_by":       "tester2",
					"processing_version":     testData["processing_version"],
					"acquisition_era_name":   testData["acquisition_era"],
				},
				output: []Response{
					{
						"dataset": "unittest",
					},
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":             []string{testData["dataset"].(string)},
					"dataset_access_type": []string{"PRODUCTION"},
				},
				output: []Response{
					{
						"dataset": testData["dataset"],
					},
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST with detail",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":             []string{testData["dataset"].(string)},
					"dataset_access_type": []string{"PRODUCTION"},
					"detail":              []string{"true"},
				},
				output: []Response{
					{
						"acquisition_era_name":   testData["acquisition_era"],
						"create_by":              "tester",
						"creation_date":          1635177605,
						"data_tier_name":         testData["data_tier_name"],
						"dataset":                testData["dataset"],
						"dataset_access_type":    "PRODUCTION",
						"dataset_id":             1.0,
						"last_modification_date": 1635177605,
						"last_modified_by":       "tester2",
						"physics_group_name":     "Tracker",
						"prep_id":                "",
						"primary_ds_name":        testData["primary_ds_name"],
						"primary_ds_type":        "test",
						"processed_ds_name":      testData["processed_ds_name"],
						"processing_version":     testData["processing_version"],
						"xtcrosssection":         123.0,
					},
				},
				respCode: http.StatusOK,
			},
		},
	}

	return []EndpointTestCase{
		primaryDatasetAndTypesTestCase,
		outputConfigTestCase,
		acquisitionErasTestCase,
		processingErasTestCase,
		datatiersTestCase,
		datasetAccessTypesTestCase,
		physicsGroupsTestCase,
		datasetsTestCase,
	}
}
