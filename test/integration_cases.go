package main

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/google/uuid"
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

// testData struct for test data generation
type testData struct {
	primary_ds_name          string
	procdataset              string
	tier                     string
	dataset                  string
	parent_dataset           string
	primary_ds_name2         string
	dataset2                 string
	app_name                 string
	output_module_label      string
	global_tag               string
	pset_hash                string
	pset_name                string
	release_version          string
	site                     string
	block                    string
	parent_block             string
	files                    []string
	parent_files             []string
	runs                     []int
	acquisition_era          string
	processing_version       float64
	step_primary_ds_name     string
	stepchain_dataset        string
	stepchain_block          string
	parent_stepchain_dataset string
	parent_stepchain_block   string
	stepchain_files          []string
	parent_stepchain_files   []string
}

// TestData contains the generated data
var TestData testData

// defines a testcase for an endpoint
type EndpointTestCase struct {
	description     string
	defaultHandler  func(http.ResponseWriter, *http.Request)
	defaultEndpoint string
	testCases       []testCase
}

// get a UUID time_mid as an int
func getUUID(t *testing.T) int64 {
	uuid, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	time_mid := uuid[4:6]
	uid := hex.EncodeToString(time_mid)
	uidInt, err := strconv.ParseInt(uid, 16, 64)
	if err != nil {
		t.Fatal(err)
	}

	return uidInt
}

// generate test data
func generateBaseData(t *testing.T) {
	uid := getUUID(t)
	fmt.Printf("****uid=%v*****\n", uid)

	primary_ds_name := fmt.Sprintf("unittest_web_primary_ds_name_%v", uid)
	processing_version := uid % 9999
	if uid < 9999 {
		processing_version = uid
	}
	acquisition_era_name := fmt.Sprintf("acq_era_%v", uid)
	procdataset := fmt.Sprintf("%s-pstr-v%v", acquisition_era_name, processing_version)
	parent_procdataset := fmt.Sprintf("%s-ptsr-v%v", acquisition_era_name, processing_version)
	tier := "GEN-SIM-RAW"
	dataset := fmt.Sprintf("/%s/%s/%s", primary_ds_name, procdataset, tier)
	primary_ds_name2 := fmt.Sprintf("%s_2", primary_ds_name)
	dataset2 := fmt.Sprintf("/%s/%s/%s", primary_ds_name2, procdataset, tier)
	app_name := "cmsRun"
	output_module_label := "merged"
	global_tag := fmt.Sprintf("my-cms-gtag_%v", uid)
	pset_hash := "76e303993a1c2f842159dbfeeed9a0dd"
	pset_name := "UnittestPsetName"
	release_version := "CMSSW_1_2_3"
	site := "cmssrm.fnal.gov"
	block := fmt.Sprintf("%s#%v", dataset, uid)
	parent_dataset := fmt.Sprintf("/%s/%s/%s", primary_ds_name, parent_procdataset, tier)
	parent_block := fmt.Sprintf("%s#%v", parent_dataset, uid)

	step_primary_ds_name := fmt.Sprintf("%s_stepchain", primary_ds_name)
	stepchain_dataset := fmt.Sprintf("/%s/%s/%s", step_primary_ds_name, procdataset, tier)
	stepchain_block := fmt.Sprintf("%s#%v", stepchain_dataset, uid)
	parent_stepchain_dataset := fmt.Sprintf("/%s/%s/%s", step_primary_ds_name, parent_procdataset, tier)
	parent_stepchain_block := fmt.Sprintf("%s#%v", parent_stepchain_dataset, uid)
	stepchain_files := []string{}
	parent_stepchain_files := []string{}

	TestData.primary_ds_name = primary_ds_name
	TestData.procdataset = procdataset
	TestData.tier = tier
	TestData.dataset = dataset
	TestData.parent_dataset = parent_dataset
	TestData.primary_ds_name2 = primary_ds_name2
	TestData.dataset2 = dataset2
	TestData.app_name = app_name
	TestData.output_module_label = output_module_label
	TestData.global_tag = global_tag
	TestData.pset_hash = pset_hash
	TestData.pset_name = pset_name
	TestData.release_version = release_version
	TestData.site = site
	TestData.block = block
	TestData.parent_block = parent_block
	TestData.files = []string{}
	TestData.parent_files = []string{}
	TestData.runs = []int{97, 98, 99}
	TestData.acquisition_era = acquisition_era_name
	TestData.processing_version = float64(processing_version)
	TestData.step_primary_ds_name = step_primary_ds_name
	TestData.stepchain_dataset = stepchain_dataset
	TestData.stepchain_block = stepchain_block
	TestData.parent_stepchain_dataset = parent_stepchain_dataset
	TestData.parent_stepchain_block = parent_stepchain_block
	TestData.stepchain_files = stepchain_files
	TestData.parent_stepchain_files = parent_stepchain_files

	fmt.Println(TestData)
}

// primarydataset and primarydstype endpoints tests
func getPrimaryDatasetTestTable(t *testing.T) EndpointTestCase {
	primaryDSRequest := RequestBody{
		"primary_ds_name": TestData.primary_ds_name,
		"primary_ds_type": "test",
		"create_by":       "tester",
	}
	primaryDSResponse := Response{
		"primary_ds_id":   1.0,
		"primary_ds_name": TestData.primary_ds_name,
		"creation_date":   0,
		"create_by":       "tester",
		"primary_ds_type": "test",
	}
	primaryDSTypeResponse := Response{
		"primary_ds_type_id": 1.0,
		"data_type":          "test",
	}
	primaryDSRequest2 := RequestBody{
		"primary_ds_name": TestData.primary_ds_name2,
		"primary_ds_type": "test",
		"create_by":       "tester",
	}
	primaryDSResponse2 := Response{
		"primary_ds_id":   2.0,
		"primary_ds_name": TestData.primary_ds_name2,
		"creation_date":   0,
		"create_by":       "tester",
		"primary_ds_type": "test",
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
				input: RequestBody{
					"bad-field": "Bad",
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
				input:       primaryDSRequest,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResponse,
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
					primaryDSTypeResponse,
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
					primaryDSTypeResponse,
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
					primaryDSTypeResponse,
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
					primaryDSTypeResponse,
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
				input:       primaryDSRequest,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after duplicate POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResponse,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydataset second POST",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input:       primaryDSRequest2,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after second POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResponse,
					primaryDSResponse2,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// outputconfigs endpoint tests
// TODO: Rest of test cases
func getOutputConfigTestTable(t *testing.T) EndpointTestCase {
	outputConfigRequest := RequestBody{
		"app_name":            TestData.app_name,
		"release_version":     TestData.release_version,
		"pset_hash":           TestData.pset_hash,
		"global_tag":          TestData.global_tag,
		"output_module_label": TestData.output_module_label,
		"create_by":           "tester",
		"scenario":            "note",
	}
	outputConfigResponse := Response{
		"app_name":            TestData.app_name,
		"release_version":     TestData.release_version,
		"pset_hash":           TestData.pset_hash,
		"global_tag":          TestData.global_tag,
		"output_module_label": TestData.output_module_label,
		"create_by":           "tester",
		"creation_date":       0,
	}
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
				input:       outputConfigRequest,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test duplicate POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       outputConfigRequest,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					outputConfigResponse,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
		},
	}
}

// LoadTestCases loads the testData from a json file
func LoadTestCases(t *testing.T) []EndpointTestCase {
	generateBaseData(t)
	var primaryDatasetAndTypesTestCase EndpointTestCase
	var outputConfigTestCase EndpointTestCase
	var acquisitionErasTestCase EndpointTestCase
	var processingErasTestCase EndpointTestCase
	var datatiersTestCase EndpointTestCase
	var datasetAccessTypesTestCase EndpointTestCase
	var physicsGroupsTestCase EndpointTestCase
	var datasetsTestCase EndpointTestCase

	primaryDatasetAndTypesTestCase = getPrimaryDatasetTestTable(t)
	outputConfigTestCase = getOutputConfigTestTable(t)

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
					"acquisition_era_name": TestData.acquisition_era,
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
						"acquisition_era_name": TestData.acquisition_era,
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
					"processing_version": TestData.processing_version,
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
						"processing_version": TestData.processing_version,
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
					"non-existing-field": TestData.tier,
				},
				params:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: RequestBody{
					"data_tier_name": TestData.tier,
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_id":   "1",
						"data_tier_name": TestData.tier,
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
					"data_tier_name": TestData.tier,
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_name": TestData.tier,
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
					"data_tier_name": TestData.tier,
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_name": TestData.tier,
						"create_by":      "tester",
						"creation_date":  "0",
						"data_tier_id":   1.0,
					},
				},
				params: url.Values{
					"data_tier_name": []string{
						TestData.tier,
					},
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with regex parameter",
				method:      "GET",
				serverType:  "DBSReader",
				input: RequestBody{
					"data_tier_name": TestData.tier,
					"create_by":      "tester",
				},
				output: []Response{
					{
						"data_tier_name": TestData.tier,
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
					"data_tier_name": TestData.tier,
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
					"dataset":             TestData.dataset,
					"dataset_access_type": "PRODUCTION",
					"processed_ds_name":   TestData.procdataset,
					"primary_ds_name":     TestData.primary_ds_name,
					"output_configs": []RequestBody{
						{
							"release_version":     TestData.release_version,
							"pset_hash":           TestData.pset_hash,
							"app_name":            TestData.app_name,
							"output_module_label": TestData.output_module_label,
							"global_tag":          TestData.global_tag,
						},
					},
					"xtcrosssection":         123,
					"primary_ds_type":        "test",
					"data_tier_name":         TestData.tier,
					"creation_date":          1635177605,
					"create_by":              "tester",
					"last_modification_date": 1635177605,
					"last_modified_by":       "testuser",
					"processing_version":     TestData.processing_version,
					"acquisition_era_name":   TestData.acquisition_era,
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
					"dataset":             TestData.dataset,
					"dataset_access_type": "PRODUCTION",
					"processed_ds_name":   TestData.procdataset,
					"primary_ds_name":     TestData.primary_ds_name,
					"output_configs": []RequestBody{
						{
							"release_version":     TestData.release_version,
							"pset_hash":           TestData.pset_hash,
							"app_name":            TestData.app_name,
							"output_module_label": TestData.output_module_label,
							"global_tag":          TestData.global_tag,
						},
					},
					"xtcrosssection":         123,
					"primary_ds_type":        "test",
					"data_tier_name":         TestData.tier,
					"creation_date":          1635177605,
					"create_by":              "tester",
					"last_modification_date": 1635177605,
					"last_modified_by":       "testuser",
					"processing_version":     TestData.processing_version,
					"acquisition_era_name":   TestData.acquisition_era,
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
					"dataset":             []string{TestData.dataset},
					"dataset_access_type": []string{"PRODUCTION"},
				},
				output: []Response{
					{
						"dataset": TestData.dataset,
					},
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST with detail",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":             []string{TestData.dataset},
					"dataset_access_type": []string{"PRODUCTION"},
					"detail":              []string{"true"},
				},
				output: []Response{
					{
						"acquisition_era_name":   TestData.acquisition_era,
						"create_by":              "tester",
						"creation_date":          1635177605,
						"data_tier_name":         TestData.tier,
						"dataset":                TestData.dataset,
						"dataset_access_type":    "PRODUCTION",
						"dataset_id":             1.0,
						"last_modification_date": 1635177605,
						"last_modified_by":       "testuser",
						"physics_group_name":     "Tracker",
						"prep_id":                "",
						"primary_ds_name":        TestData.primary_ds_name,
						"primary_ds_type":        "test",
						"processed_ds_name":      TestData.procdataset,
						"processing_version":     TestData.processing_version,
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
