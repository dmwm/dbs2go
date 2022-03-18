package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
)

// Response represents an expected HTTP response body
type Response interface{}

// RequestBody represents an expected HTTP request body
type RequestBody interface{}

// BadRequest represents a request with no good fields
type BadRequest struct {
	BAD_FIELD string
}

// PrimaryDSTypesResponse is the expected primarydstypes GET response
type PrimaryDSTypesResponse struct {
	DATA_TYPE          string `json:"data_type"`
	PRIMARY_DS_TYPE_ID int64  `json:"primary_ds_type_id"`
}

// OutputConfigResponse is the expected outputconfigs GET response
type OutputConfigResponse struct {
	APP_NAME            string `json:"app_name"`
	RELEASE_VERSION     string `json:"release_version"`
	PSET_HASH           string `json:"pset_hash"`
	PSET_NAME           string `json:"pset_name"`
	GLOBAL_TAG          string `json:"global_tag"`
	OUTPUT_MODULE_LABEL string `json:"output_module_label"`
	CREATION_DATE       int64  `json:"creation_date"`
	CREATE_BY           string `json:"create_by"`
}

// basic elements to define a test case
type testCase struct {
	description string     // test case description
	serverType  string     //DBSWriter, DBSReader, DBSMigrate
	method      string     // http method
	endpoint    string     // url endpoint
	params      url.Values // url parameters
	handler     func(http.ResponseWriter, *http.Request)
	input       RequestBody //kjdPOST record
	output      []Response  // expected response
	respCode    int         // expected HTTP response code
}

// InitialData struct for test data generation
type InitialData struct {
	PrimaryDSName          string   `json:"primary_ds_name"`
	ProcDataset            string   `json:"procdataset"`
	Tier                   string   `json:"tier"`
	Dataset                string   `json:"dataset"`
	ParentDataset          string   `json:"parent_dataset"`
	PrimaryDSName2         string   `json:"primary_ds_name2"`
	Dataset2               string   `json:"dataset2"`
	AppName                string   `json:"app_name"`
	OutputModuleLabel      string   `json:"output_module_label"`
	GlobalTag              string   `json:"global_tag"`
	PsetHash               string   `json:"pset_hash"`
	PsetName               string   `json:"pset_name"`
	ReleaseVersion         string   `json:"release_version"`
	Site                   string   `json:"site"`
	Block                  string   `json:"block"`
	ParentBlock            string   `json:"parent_block"`
	Files                  []string `json:"files"`
	ParentFiles            []string `json:"parent_files"`
	Runs                   []int    `json:"runs"`
	AcquisitionEra         string   `json:"acquisition_era"`
	ProcessingVersion      float64  `json:"processing_version"`
	StepPrimaryDSName      string   `json:"step_primary_ds_name"`
	StepchainDataset       string   `json:"stepchain_dataset"`
	StepchainBlock         string   `json:"stepchain_block"`
	ParentStepchainDataset string   `json:"parent_stepchain_dataset"`
	ParentStepchainBlock   string   `json:"parent_stepchain_block"`
	StepchainFiles         []string `json:"stepchain_files"`
	ParentStepchainFiles   []string `json:"parent_stepchain_files"`
}

// TestData contains the generated data
var TestData InitialData

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

	PrimaryDSName := fmt.Sprintf("unittest_web_primary_ds_name_%v", uid)
	processing_version := uid % 9999
	if uid < 9999 {
		processing_version = uid
	}
	acquisition_era_name := fmt.Sprintf("acq_era_%v", uid)
	ProcDataset := fmt.Sprintf("%s-pstr-v%v", acquisition_era_name, processing_version)
	parent_procdataset := fmt.Sprintf("%s-ptsr-v%v", acquisition_era_name, processing_version)
	Tier := "GEN-SIM-RAW"
	dataset := fmt.Sprintf("/%s/%s/%s", PrimaryDSName, ProcDataset, Tier)
	primary_ds_name2 := fmt.Sprintf("%s_2", PrimaryDSName)
	dataset2 := fmt.Sprintf("/%s/%s/%s", primary_ds_name2, ProcDataset, Tier)
	app_name := "cmsRun"
	output_module_label := "merged"
	global_tag := fmt.Sprintf("my-cms-gtag_%v", uid)
	pset_hash := "76e303993a1c2f842159dbfeeed9a0dd"
	pset_name := "UnittestPsetName"
	release_version := "CMSSW_1_2_3"
	site := "cmssrm.fnal.gov"
	block := fmt.Sprintf("%s#%v", dataset, uid)
	parent_dataset := fmt.Sprintf("/%s/%s/%s", PrimaryDSName, parent_procdataset, Tier)
	parent_block := fmt.Sprintf("%s#%v", parent_dataset, uid)

	step_primary_ds_name := fmt.Sprintf("%s_stepchain", PrimaryDSName)
	stepchain_dataset := fmt.Sprintf("/%s/%s/%s", step_primary_ds_name, ProcDataset, Tier)
	stepchain_block := fmt.Sprintf("%s#%v", stepchain_dataset, uid)
	parent_stepchain_dataset := fmt.Sprintf("/%s/%s/%s", step_primary_ds_name, parent_procdataset, Tier)
	parent_stepchain_block := fmt.Sprintf("%s#%v", parent_stepchain_dataset, uid)
	stepchain_files := []string{}
	parent_stepchain_files := []string{}

	TestData.PrimaryDSName = PrimaryDSName
	TestData.ProcDataset = ProcDataset
	TestData.Tier = Tier
	TestData.Dataset = dataset
	TestData.ParentDataset = parent_dataset
	TestData.PrimaryDSName2 = primary_ds_name2
	TestData.Dataset2 = dataset2
	TestData.AppName = app_name
	TestData.OutputModuleLabel = output_module_label
	TestData.GlobalTag = global_tag
	TestData.PsetHash = pset_hash
	TestData.PsetName = pset_name
	TestData.ReleaseVersion = release_version
	TestData.Site = site
	TestData.Block = block
	TestData.ParentBlock = parent_block
	TestData.Files = []string{}
	TestData.ParentFiles = []string{}
	TestData.Runs = []int{97, 98, 99}
	TestData.AcquisitionEra = acquisition_era_name
	TestData.ProcessingVersion = float64(processing_version)
	TestData.StepPrimaryDSName = step_primary_ds_name
	TestData.StepchainDataset = stepchain_dataset
	TestData.StepchainBlock = stepchain_block
	TestData.ParentStepchainDataset = parent_stepchain_dataset
	TestData.ParentStepchainBlock = parent_stepchain_block
	TestData.StepchainFiles = stepchain_files
	TestData.ParentStepchainFiles = parent_stepchain_files

	fmt.Println(TestData)
	file, _ := json.MarshalIndent(TestData, "", "  ")
	fmt.Println(file)
	_ = ioutil.WriteFile("./data/integration/integration_data.json", file, os.ModePerm)
}

// primarydataset and primarydstype endpoints tests
func getPrimaryDatasetTestTable(t *testing.T) EndpointTestCase {
	// create data structs for expected requests and responses
	primaryDSRequest := dbs.PrimaryDatasetRecord{
		PRIMARY_DS_NAME: TestData.PrimaryDSName,
		PRIMARY_DS_TYPE: "test",
		CREATE_BY:       "tester",
	}
	primaryDSResponse := dbs.PrimaryDataset{
		PrimaryDSId:   1.0,
		PrimaryDSType: "test",
		PrimaryDSName: TestData.PrimaryDSName,
		CreationDate:  0,
		CreateBy:      "tester",
	}
	primaryDSTypeResponse := PrimaryDSTypesResponse{
		PRIMARY_DS_TYPE_ID: 1.0,
		DATA_TYPE:          "test",
	}
	primaryDSRequest2 := dbs.PrimaryDatasetRecord{
		PRIMARY_DS_NAME: TestData.PrimaryDSName2,
		PRIMARY_DS_TYPE: "test",
		CREATE_BY:       "tester",
	}
	primaryDSResponse2 := dbs.PrimaryDataset{
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
	errorResponse := web.ServerError{
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
					errorResponse,
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
	outputConfigRequest := dbs.OutputConfigRecord{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           "tester",
		SCENARIO:            "note",
	}
	outputConfigResponse := OutputConfigResponse{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           "tester",
		CREATION_DATE:       0,
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
				input: BadRequest{
					BAD_FIELD: "bad",
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

// acquisitioneras endpoint tests
// TODO: Rest of test cases
func getAcquisitionErasTestTable(t *testing.T) EndpointTestCase {
	acqEraRequest := dbs.AcquisitionEras{
		ACQUISITION_ERA_NAME: TestData.AcquisitionEra,
		DESCRIPTION:          "note",
		CREATE_BY:            "tester",
	}
	acqEraResponse := dbs.AcquisitionEra{
		AcquisitionEraName: TestData.AcquisitionEra,
		StartDate:          0,
		EndDate:            0,
		CreationDate:       0,
		CreateBy:           "tester",
		Description:        "note",
	}
	return EndpointTestCase{
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
				input:       acqEraRequest,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					acqEraResponse,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// processingeras endpoint tests
// TODO: Rest of test cases
func getProcessingErasTestTable(t *testing.T) EndpointTestCase {
	procErasReq := dbs.ProcessingEras{
		PROCESSING_VERSION: int64(TestData.ProcessingVersion),
		DESCRIPTION:        "this_is_a_test",
		CREATE_BY:          "tester",
	}
	procErasResp := dbs.ProcessingEra{
		ProcessingVersion: int64(TestData.ProcessingVersion),
		CreateBy:          "tester",
		Description:       "this_is_a_test",
		CreationDate:      0,
	}
	return EndpointTestCase{
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
				input:       procErasReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					procErasResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// LoadTestCases loads the InitialData from a json file
func LoadTestCases(t *testing.T) []EndpointTestCase {
	if _, err := os.Stat("./data/integration/integration_data.json"); err == nil {
		generateBaseData(t)
	}
	/*
		var datatiersTestCase EndpointTestCase
		var datasetAccessTypesTestCase EndpointTestCase
		var physicsGroupsTestCase EndpointTestCase
		var datasetsTestCase EndpointTestCase
	*/

	primaryDatasetAndTypesTestCase := getPrimaryDatasetTestTable(t)
	outputConfigTestCase := getOutputConfigTestTable(t)
	acquisitionErasTestCase := getAcquisitionErasTestTable(t)
	processingErasTestCase := getProcessingErasTestTable(t)

	/*
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
						"non-existing-field": TestData.Tier,
					},
					params:   nil,
					respCode: http.StatusBadRequest,
				},
				{
					description: "Test POST",
					method:      "POST",
					serverType:  "DBSWriter",
					input: RequestBody{
						"data_tier_name": TestData.Tier,
						"create_by":      "tester",
					},
					output: []Response{
						{
							"data_tier_id":   "1",
							"data_tier_name": TestData.Tier,
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
						"data_tier_name": TestData.Tier,
						"create_by":      "tester",
					},
					output: []Response{
						{
							"data_tier_name": TestData.Tier,
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
						"data_tier_name": TestData.Tier,
						"create_by":      "tester",
					},
					output: []Response{
						{
							"data_tier_name": TestData.Tier,
							"create_by":      "tester",
							"creation_date":  "0",
							"data_tier_id":   1.0,
						},
					},
					params: url.Values{
						"data_tier_name": []string{
							TestData.Tier,
						},
					},
					respCode: http.StatusOK,
				},
				{
					description: "Test GET with regex parameter",
					method:      "GET",
					serverType:  "DBSReader",
					input: RequestBody{
						"data_tier_name": TestData.Tier,
						"create_by":      "tester",
					},
					output: []Response{
						{
							"data_tier_name": TestData.Tier,
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
						"data_tier_name": TestData.Tier,
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
						"processed_ds_name":   TestData.ProcDataset,
						"PrimaryDSName":     TestData.PrimaryDSName,
						"output_configs": []RequestBody{
							{
								"release_version":     TestData.ReleaseVersion,
								"pset_hash":           TestData.PsetHash,
								"app_name":            TestData.AppName,
								"output_module_label": TestData.OutputModuleLabel,
								"global_tag":          TestData.GlobalTag,
							},
						},
						"xtcrosssection":         123,
						"primary_ds_type":        "test",
						"data_tier_name":         TestData.Tier,
						"creation_date":          1635177605,
						"create_by":              "tester",
						"last_modification_date": 1635177605,
						"last_modified_by":       "testuser",
						"processing_version":     TestData.ProcessingVersion,
						"acquisition_era_name":   TestData.AcquisitionEra,
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
						"processed_ds_name":   TestData.ProcDataset,
						"PrimaryDSName":     TestData.PrimaryDSName,
						"output_configs": []RequestBody{
							{
								"release_version":     TestData.ReleaseVersion,
								"pset_hash":           TestData.PsetHash,
								"app_name":            TestData.AppName,
								"output_module_label": TestData.OutputModuleLabel,
								"global_tag":          TestData.GlobalTag,
							},
						},
						"xtcrosssection":         123,
						"primary_ds_type":        "test",
						"data_tier_name":         TestData.Tier,
						"creation_date":          1635177605,
						"create_by":              "tester",
						"last_modification_date": 1635177605,
						"last_modified_by":       "testuser",
						"processing_version":     TestData.ProcessingVersion,
						"acquisition_era_name":   TestData.AcquisitionEra,
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
							"acquisition_era_name":   TestData.AcquisitionEra,
							"create_by":              "tester",
							"creation_date":          1635177605,
							"data_tier_name":         TestData.Tier,
							"dataset":                TestData.dataset,
							"dataset_access_type":    "PRODUCTION",
							"dataset_id":             1.0,
							"last_modification_date": 1635177605,
							"last_modified_by":       "testuser",
							"physics_group_name":     "Tracker",
							"prep_id":                "",
							"PrimaryDSName":        TestData.PrimaryDSName,
							"primary_ds_type":        "test",
							"processed_ds_name":      TestData.ProcDataset,
							"processing_version":     TestData.ProcessingVersion,
							"xtcrosssection":         123.0,
						},
					},
					respCode: http.StatusOK,
				},
			},
		}
	*/

	return []EndpointTestCase{
		primaryDatasetAndTypesTestCase,
		outputConfigTestCase,
		acquisitionErasTestCase,
		processingErasTestCase,
		/*
			datatiersTestCase,
			datasetAccessTypesTestCase,
			physicsGroupsTestCase,
			datasetsTestCase,
		*/
	}
}
