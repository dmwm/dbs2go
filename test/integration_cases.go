package main

// This file contains common types for test cases. It also contains a function to generate initial data, if the json file does not exist
// the initial data is also loaded into the TestData struct for the test-driven tables defined in  int_*.go

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/google/uuid"
)

// Response represents an expected HTTP response body
type Response interface{}

// RequestBody represents an expected HTTP request body
type RequestBody interface{}

// BadRequest represents a request with no good fields
type BadRequest struct {
	BAD_FIELD string
}

// basic elements to define a test case
type testCase struct {
	description          string                                     // test case description
	serverType           string                                     // DBSWriter, DBSReader, DBSMigrate
	concurrentBulkBlocks bool                                       // true for concurrentBulkBlocks
	fileLumiChunkSize    int                                        // if not used, will default to EndpointTestCase value
	method               string                                     // http method
	endpoint             string                                     // url endpoint, optional if EndpointTestCase.defaultEndpoint is defined
	params               url.Values                                 // url parameters, optional
	handler              func(http.ResponseWriter, *http.Request)   // optional if EndpointTestCase.defaultHandler is defined
	input                RequestBody                                // POST and PUT body, optional for GET request
	output               []Response                                 // expected response
	respCode             int                                        // expected HTTP response code
	verifyFunc           func(*testing.T, []dbs.Record, []Response) // function to verify a response
}

// initialData struct for test data generation
type initialData struct {
	UID                    int64    `json:"uid"`
	CreateBy               string   `json:"create_by"`
	PrimaryDSName          string   `json:"primary_ds_name"`
	PrimaryDSType          string   `json:"primary_ds_type"`
	ProcDataset            string   `json:"procdataset"`
	PhysicsGroupName       string   `json:"physics_group_name"`
	DatasetAccessType      string   `json:"dataset_access_type"`
	DatasetAccessType2     string   `json:"dataset_access_type2"`
	Tier                   string   `json:"tier"`
	Dataset                string   `json:"dataset"`
	ParentDataset          string   `json:"parent_dataset"`
	ParentProcDataset      string   `json:"parent_procdataset"`
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
	ParentBlockName        string   `json:"parent_block_name"`
	Files                  []string `json:"files"`
	ParentFiles            []string `json:"parent_files"`
	Runs                   []int    `json:"runs"`
	AcquisitionEra         string   `json:"acquisition_era"`
	ProcessingVersion      int64    `json:"processing_version"`
	StepPrimaryDSName      string   `json:"step_primary_ds_name"`
	StepchainDataset       string   `json:"stepchain_dataset"`
	StepchainBlock         string   `json:"stepchain_block"`
	ParentStepchainDataset string   `json:"parent_stepchain_dataset"`
	ParentStepchainBlock   string   `json:"parent_stepchain_block"`
	StepchainFiles         []string `json:"stepchain_files"`
	ParentStepchainFiles   []string `json:"parent_stepchain_files"`
}

// struct containing bulk blocks data
type bulkBlocksData struct {
	ConcurrentParentData dbs.BulkBlocks `json:"con_parent_bulk"` // for concurrent bulkblocks
	ConcurrentChildData  dbs.BulkBlocks `json:"con_child_bulk"`  // for concurrent bulkblocks
	SequentialParentData dbs.BulkBlocks `json:"seq_parent_bulk"` // for sequential bulkblocks
	SequentialChildData  dbs.BulkBlocks `json:"seq_child_bulk"`  // for sequential bulkblocks
}

// TestData contains the generated data
var TestData initialData

// BulkBlocksData contains data for bulkblocks
// TestData must first be filled
var BulkBlocksData bulkBlocksData

// LargeBulkBlocksData contains data for fileLumiChunkSize test
var LargeBulkBlocksData dbs.BulkBlocks

// defines a testcase for an endpoint
type EndpointTestCase struct {
	description       string                                   // test description
	defaultHandler    func(http.ResponseWriter, *http.Request) // default handler for GET in POST
	defaultEndpoint   string                                   // default endpoint for requests
	fileLumiChunkSize int                                      // if not used, will default to 500
	testCases         []testCase                               // test cases
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

// generate test data based on dmwm/DBSClient tests/dbsclient_t/unittests/DBSClientWriter_t.py
func generateBaseData(t *testing.T, filepath string) {
	uid := getUUID(t)
	fmt.Printf("****uid=%v*****\n", uid)

	PrimaryDSName := fmt.Sprintf("unittest_web_primary_ds_name_%v", uid)
	processing_version := uid % 9999
	if uid < 9999 {
		processing_version = uid
	}
	acquisition_era_name := fmt.Sprintf("acq_era_%v", uid)
	ProcDataset := fmt.Sprintf("%s-v%v", acquisition_era_name, processing_version)
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

	TestData.CreateBy = "tester"
	TestData.PrimaryDSType = "test"
	TestData.PhysicsGroupName = "Tracker"
	TestData.DatasetAccessType = "VALID"
	TestData.DatasetAccessType2 = "PRODUCTION"

	TestData.UID = uid
	TestData.PrimaryDSName = PrimaryDSName
	TestData.ProcDataset = ProcDataset
	TestData.Tier = Tier
	TestData.Dataset = dataset
	TestData.ParentDataset = parent_dataset
	TestData.ParentProcDataset = parent_procdataset
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
	TestData.ParentBlockName = parent_block
	TestData.Files = []string{}
	TestData.ParentFiles = []string{}
	TestData.Runs = []int{97, 98, 99}
	TestData.AcquisitionEra = acquisition_era_name
	TestData.ProcessingVersion = processing_version
	TestData.StepPrimaryDSName = step_primary_ds_name
	TestData.StepchainDataset = stepchain_dataset
	TestData.StepchainBlock = stepchain_block
	TestData.ParentStepchainDataset = parent_stepchain_dataset
	TestData.ParentStepchainBlock = parent_stepchain_block
	TestData.StepchainFiles = stepchain_files
	TestData.ParentStepchainFiles = parent_stepchain_files

	// fmt.Println(TestData)
	file, _ := json.MarshalIndent(TestData, "", "  ")
	_ = ioutil.WriteFile(filepath, file, os.ModePerm)
}

// creates a file for bulkblocks
func createFile(t *testing.T, i int) dbs.File {
	return dbs.File{
		Adler32:          "NOTSET",
		FileType:         "EDM",
		FileSize:         2012211901,
		AutoCrossSection: 0.0,
		CheckSum:         "1504266448",
		FileLumiList: []dbs.FileLumi{
			{
				LumiSectionNumber: int64(27414 + i),
				RunNumber:         98,
				EventCount:        66,
			},
			{
				LumiSectionNumber: int64(26422 + i),
				RunNumber:         98,
				EventCount:        67,
			},
			{
				LumiSectionNumber: int64(29838 + i),
				RunNumber:         98,
				EventCount:        68,
			},
		},
		EventCount:      201,
		LogicalFileName: fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/StepChain_/p%v/%v.root", TestData.UID, i),
		IsFileValid:     1,
	}
}

// generates bulkblocks data
func generateBulkBlocksData(t *testing.T, filepath string) {
	var parentBulk dbs.BulkBlocks
	var parentBulk2 dbs.BulkBlocks
	var bulk dbs.BulkBlocks
	var bulk2 dbs.BulkBlocks
	var primDS dbs.PrimaryDataset
	var dataset dbs.Dataset
	var processingEra dbs.ProcessingEra
	var acqEra dbs.AcquisitionEra
	var block dbs.Block

	algo := dbs.DatasetConfig{
		ReleaseVersion:    TestData.ReleaseVersion,
		PsetHash:          TestData.PsetHash,
		AppName:           TestData.AppName,
		OutputModuleLabel: TestData.OutputModuleLabel,
		GlobalTag:         TestData.GlobalTag,
	}

	primDS.PrimaryDSName = TestData.StepPrimaryDSName
	primDS.PrimaryDSType = "test"
	primDS.CreateBy = "WMAgent"
	primDS.CreationDate = time.Now().Unix() // Replace with fixed time

	dataset = dbs.Dataset{
		PhysicsGroupName:     TestData.PhysicsGroupName,
		ProcessedDSName:      TestData.ProcDataset,
		DataTierName:         TestData.Tier,
		DatasetAccessType:    TestData.DatasetAccessType2,
		Dataset:              TestData.StepchainDataset,
		PrepID:               "TestPrepID",
		CreateBy:             "WMAgent",
		LastModifiedBy:       "WMAgent",
		CreationDate:         time.Now().Unix(),
		LastModificationDate: time.Now().Unix(),
	}

	processingEra = dbs.ProcessingEra{
		ProcessingVersion: TestData.ProcessingVersion,
		CreateBy:          "WMAgent",
	}

	acqEra = dbs.AcquisitionEra{
		AcquisitionEraName: TestData.AcquisitionEra,
		StartDate:          123456789,
	}

	fileCount := 5

	block = dbs.Block{
		BlockName:      TestData.StepchainBlock,
		OriginSiteName: TestData.Site,
		FileCount:      int64(fileCount),
		BlockSize:      20122119010,
	}

	bulk.DatasetConfigList = []dbs.DatasetConfig{algo}
	bulk.PrimaryDataset = primDS
	bulk.Dataset = dataset
	bulk.ProcessingEra = processingEra
	bulk.AcquisitionEra = acqEra
	bulk.DatasetParentList = []string{TestData.ParentStepchainDataset}
	bulk.Block = block

	parentBulk = bulk
	parentBulk.Dataset.Dataset = TestData.ParentStepchainDataset
	parentBulk.Block.BlockName = TestData.ParentStepchainBlock
	parentBulk.DatasetParentList = []string{}
	parentBulk.PrimaryDataset.PrimaryDSName = TestData.StepPrimaryDSName
	parentBulk.Dataset.ProcessedDSName = TestData.ParentProcDataset

	bulk2 = bulk
	bulk2.Dataset.Dataset = bulk2.Dataset.Dataset + "2"
	bulk2.Block.BlockName = bulk2.Block.BlockName + "2"
	bulk2.DatasetParentList = []string{TestData.ParentStepchainDataset + "2"}
	bulk2.PrimaryDataset.PrimaryDSName = bulk2.PrimaryDataset.PrimaryDSName + "2"
	bulk2.Dataset.ProcessedDSName = bulk2.Dataset.ProcessedDSName + "2"

	parentBulk2 = bulk2
	parentBulk2.Dataset.Dataset = TestData.ParentStepchainDataset + "2"
	parentBulk2.Block.BlockName = TestData.ParentStepchainBlock + "2"
	parentBulk2.DatasetParentList = []string{}
	parentBulk2.PrimaryDataset.PrimaryDSName = TestData.StepPrimaryDSName + "2"
	parentBulk2.Dataset.ProcessedDSName = TestData.ParentProcDataset + "2"

	var parentFileList []dbs.File
	var childFileList []dbs.File
	var parentFileList2 []dbs.File
	var childFileList2 []dbs.File
	for i := 0; i < fileCount; i++ {
		f := createFile(t, i)
		parentFileList = append(parentFileList, f)

		var parentAlgo dbs.FileConfig

		pa, err := json.Marshal(algo) // convert DatasetConfig to FileConfig
		if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(pa, &parentAlgo)

		parentAlgo.LFN = f.LogicalFileName
		parentBulk.FileConfigList = append(parentBulk.FileConfigList, parentAlgo)

		childf := f
		childf.LogicalFileName = fmt.Sprintf("/store/mc/Fall08/BBJets250t500-madgraph/GEN-SIM/StepChain_/%v/%v.root", TestData.UID, i)
		childFileList = append(childFileList, childf)

		var childAlgo dbs.FileConfig
		json.Unmarshal(pa, &childAlgo)
		childAlgo.LFN = childf.LogicalFileName
		bulk.FileConfigList = append(bulk.FileConfigList, childAlgo)

		f2 := createFile(t, i+fileCount)
		parentFileList2 = append(parentFileList2, f2)
		childf2 := f2
		childFileList2 = append(childFileList2, childf2)
	}
	parentBulk.Files = parentFileList
	bulk.Files = childFileList

	parentBulk2.Files = parentFileList2
	bulk2.Files = childFileList2

	BulkBlocksData = bulkBlocksData{
		ConcurrentParentData: parentBulk,
		ConcurrentChildData:  bulk,
		SequentialParentData: parentBulk2,
		SequentialChildData:  bulk2,
	}

	file, err := json.MarshalIndent(BulkBlocksData, "", "  ")
	if err != nil {
		t.Fatal(err.Error())
	}
	_ = ioutil.WriteFile(filepath, file, os.ModePerm)
}

// generate an individual fileLumi
func generateFileLumi(t *testing.T, i int) dbs.FileLumi {
	return dbs.FileLumi{
		LumiSectionNumber: int64(22800 + i),
		RunNumber:         100,
		EventCount:        70,
	}
}

// generates bulkblocks data with the # of file_lumi_list based on the fileLumiChunkSize
func generateLargeBulkBlocksData(t *testing.T, fileLumiChunkSize int, filepath string) {
	var parentBulk dbs.BulkBlocks
	var bulk dbs.BulkBlocks
	var primDS dbs.PrimaryDataset
	var dataset dbs.Dataset
	var processingEra dbs.ProcessingEra
	var acqEra dbs.AcquisitionEra
	var block dbs.Block

	algo := dbs.DatasetConfig{
		ReleaseVersion:    TestData.ReleaseVersion,
		PsetHash:          TestData.PsetHash,
		AppName:           TestData.AppName,
		OutputModuleLabel: TestData.OutputModuleLabel,
		GlobalTag:         TestData.GlobalTag,
	}

	primDS.PrimaryDSName = TestData.StepPrimaryDSName
	primDS.PrimaryDSType = "test"
	primDS.CreateBy = "WMAgent"
	primDS.CreationDate = time.Now().Unix() // Replace with fixed time

	dataset = dbs.Dataset{
		PhysicsGroupName:     TestData.PhysicsGroupName,
		ProcessedDSName:      TestData.ProcDataset,
		DataTierName:         TestData.Tier,
		DatasetAccessType:    TestData.DatasetAccessType2,
		Dataset:              TestData.StepchainDataset,
		PrepID:               "TestPrepID",
		CreateBy:             "WMAgent",
		LastModifiedBy:       "WMAgent",
		CreationDate:         time.Now().Unix(),
		LastModificationDate: time.Now().Unix(),
	}

	processingEra = dbs.ProcessingEra{
		ProcessingVersion: TestData.ProcessingVersion,
		CreateBy:          "WMAgent",
	}

	acqEra = dbs.AcquisitionEra{
		AcquisitionEraName: TestData.AcquisitionEra,
		StartDate:          123456789,
	}

	block = dbs.Block{
		BlockName:      TestData.StepchainBlock,
		OriginSiteName: TestData.Site,
		FileCount:      int64(fileLumiChunkSize),
		BlockSize:      20122119010,
	}

	bulk.DatasetConfigList = []dbs.DatasetConfig{algo}
	bulk.PrimaryDataset = primDS
	bulk.Dataset = dataset
	bulk.ProcessingEra = processingEra
	bulk.AcquisitionEra = acqEra
	bulk.DatasetParentList = []string{TestData.ParentStepchainDataset}
	bulk.Block = block

	parentBulk = bulk
	parentBulk.Dataset.Dataset = TestData.ParentStepchainDataset + "3"
	parentBulk.Block.BlockName = TestData.ParentStepchainBlock + "3"
	parentBulk.DatasetParentList = []string{}
	parentBulk.PrimaryDataset.PrimaryDSName = TestData.StepPrimaryDSName + "3"
	parentBulk.Dataset.ProcessedDSName = TestData.ParentProcDataset + "3"

	var parentFileList []dbs.File
	f := createFile(t, 1)

	var fileLumiList []dbs.FileLumi
	for i := 0; i < fileLumiChunkSize; i++ {
		fl := generateFileLumi(t, i)
		fileLumiList = append(fileLumiList, fl)
	}

	f.LogicalFileName = fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/StepChain10_/p%v/%v.root", TestData.UID, 1)

	f.FileLumiList = fileLumiList

	parentFileList = append(parentFileList, f)

	var parentAlgo dbs.FileConfig

	pa, err := json.Marshal(algo) // convert DatasetConfig to FileConfig
	if err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(pa, &parentAlgo)

	parentAlgo.LFN = f.LogicalFileName
	parentBulk.FileConfigList = append(parentBulk.FileConfigList, parentAlgo)

	parentBulk.Files = parentFileList

	file, err := json.MarshalIndent(parentBulk, "", " ")
	if err != nil {
		t.Fatal(err.Error())
	}
	_ = ioutil.WriteFile(filepath, file, os.ModePerm)
}

// reads a json file and load into TestData
func readJsonFile(t *testing.T, filename string, obj any) error {
	var data []byte
	var err error
	// var testData map[string]interface{}
	data, err = os.ReadFile(filename)
	if err != nil {
		log.Printf("ERROR: unable to read %s error %v", filename, err.Error())
		return err
	}
	err = json.Unmarshal(data, &obj)
	if err != nil {
		log.Println("unable to unmarshal received data")
		return err
	}
	return nil
}

// LoadTestCases loads the InitialData from a json file
func LoadTestCases(t *testing.T, filepath string, bulkblockspath string, largeBulkBlocksPath string, fileLumiLength int) []EndpointTestCase {
	var endpointTestCases []EndpointTestCase
	if _, err := os.Stat(filepath); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Generating base data")
		generateBaseData(t, filepath)
	}
	err := readJsonFile(t, filepath, &TestData)
	if err != nil {
		t.Fatal(err.Error())
	}
	// load bulkblocks data
	if _, err := os.Stat(bulkblockspath); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Generating bulkblocks data")
		generateBulkBlocksData(t, bulkblockspath)
	}
	err = readJsonFile(t, bulkblockspath, &BulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	// load large fileChunk bulkblocks data
	if _, err := os.Stat(largeBulkBlocksPath); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Generating large bulkblocks data")
		generateLargeBulkBlocksData(t, fileLumiLength, largeBulkBlocksPath)
	}
	err = readJsonFile(t, largeBulkBlocksPath, &LargeBulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(LargeBulkBlocksData.Files[0].FileLumiList) != fileLumiLength {
		fmt.Println("Generating new large bulkblocks data")
		generateLargeBulkBlocksData(t, fileLumiLength, largeBulkBlocksPath)

		err = readJsonFile(t, largeBulkBlocksPath, &LargeBulkBlocksData)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	primaryDatasetAndTypesTestCase := getPrimaryDatasetTestTable(t)
	outputConfigTestCase := getOutputConfigTestTable(t)
	acquisitionErasTestCase := getAcquisitionErasTestTable(t)
	processingErasTestCase := getProcessingErasTestTable(t)
	datatiersTestCase := getDatatiersTestTable(t)
	datasetAccessTypesTestCase := getDatasetAccessTypesTestTable(t)
	physicsGroupsTestCase := getPhysicsGroupsTestTable(t)
	datasetsTestCase := getDatasetsTestTable(t)
	blocksTestCase := getBlocksTestTable(t)
	filesTestCase := getFilesTestTable(t)
	datasetsTestCase2 := getDatasetsTestTable2(t)
	filesUpdateTestCase := getFilesTestTable2(t)
	datasetsUpdateTestCase := getDatasetsTestTable3(t)
	blockUpdateTestCase := getBlocksTestTable2(t)
	outputConfigTestCase2 := getOutputConfigTestTable2(t)
	datasetParentsTestCase := getDatasetParentsTestTable(t)
	bulkBlocksTest := getBulkBlocksTestTable(t)
	filesReaderTestTable := getFilesLumiListRangeTestTable(t)
	fileArrayTestTable := getFileArrayTestTable(t)
	largeFileLumiInsertTestTable := getBulkBlocksLargeFileLumiInsertTestTable(t)
	filesReaderAfterChunkTestTable := getFileLumiChunkTestTable(t)

	endpointTestCases = append(
		endpointTestCases,
		primaryDatasetAndTypesTestCase,
		outputConfigTestCase,
		acquisitionErasTestCase,
		processingErasTestCase,
		datatiersTestCase,
		datasetAccessTypesTestCase,
		physicsGroupsTestCase,
		datasetsTestCase,
		blocksTestCase,
		filesTestCase,
		datasetsTestCase2,
		filesUpdateTestCase,
		datasetsUpdateTestCase,
		blockUpdateTestCase,
		outputConfigTestCase2,
		datasetParentsTestCase,
		bulkBlocksTest,
	)
	endpointTestCases = append(endpointTestCases, filesReaderTestTable...)
	endpointTestCases = append(endpointTestCases, fileArrayTestTable...)
	endpointTestCases = append(endpointTestCases, largeFileLumiInsertTestTable)
	endpointTestCases = append(endpointTestCases, filesReaderAfterChunkTestTable)

	return endpointTestCases
}
