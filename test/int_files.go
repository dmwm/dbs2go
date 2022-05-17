package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// this file contains logic for files API
// the HTTP request body is defined by dbs.FileRecord struct defined in dbs/files.go
// the basic HTTP response body is defined by fileResponse struct in this file
// the detailed HTTP response body is defined by fileDetailResponse struct in this file
// the HTTP response body for run_num param is defined by fileRunResponse struct in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

// basic files API response
type fileResponse struct {
	LOGICAL_FILE_NAME string `json:"logical_file_name"`
}

// files API response with run_num
type fileRunResponse struct {
	LOGICAL_FILE_NAME string `json:"logical_file_name"`
	RUN_NUM           int64  `json:"run_num"`
}

// detailed files API response
type fileDetailResponse struct {
	ADLER32            string  `json:"adler32" validate:"required"`
	AUTO_CROSS_SECTION float64 `json:"auto_cross_section"`
	BLOCK_ID           int64   `json:"block_id" validate:"number,gt=0"`
	BLOCK_NAME         string  `json:"block_name"`
	// BRANCH_HASH_ID         int64   `json:"branch_hash_id"`
	CHECK_SUM              string `json:"check_sum" validate:"required"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number,gt=0"`
	DATASET                string `json:"dataset"`
	DATASET_ID             int64  `json:"dataset_id" validate:"number,gt=0"`
	EventCount             int64  `json:"event_count" validate:"number"`
	FILE_ID                int64  `json:"file_id"`
	FILE_SIZE              int64  `json:"file_size" validate:"required,number,gt=0"`
	FILE_TYPE              string `json:"file_type"`
	FILE_TYPE_ID           int64  `json:"file_type_id" validate:"number,gt=0"`
	IS_FILE_VALID          int64  `json:"is_file_valid" validate:"number"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
	LOGICAL_FILE_NAME      string `json:"logical_file_name" validate:"required"`
	MD5                    string `json:"md5"`
}

// detailed files API response with runs and EventCount
type fileDetailRunEventResponse struct {
	ADLER32            string  `json:"adler32" validate:"required"`
	AUTO_CROSS_SECTION float64 `json:"auto_cross_section"`
	BLOCK_ID           int64   `json:"block_id" validate:"number,gt=0"`
	BLOCK_NAME         string  `json:"block_name"`
	// BRANCH_HASH_ID         int64   `json:"branch_hash_id"`
	CHECK_SUM              string `json:"check_sum" validate:"required"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number,gt=0"`
	DATASET                string `json:"dataset"`
	DATASET_ID             int64  `json:"dataset_id" validate:"number,gt=0"`
	EventCount             int64  `json:"event_count" validate:"number"`
	FILE_ID                int64  `json:"file_id"`
	FILE_SIZE              int64  `json:"file_size" validate:"required,number,gt=0"`
	FILE_TYPE              string `json:"file_type"`
	FILE_TYPE_ID           int64  `json:"file_type_id" validate:"number,gt=0"`
	IS_FILE_VALID          int64  `json:"is_file_valid" validate:"number"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
	LOGICAL_FILE_NAME      string `json:"logical_file_name" validate:"required"`
	MD5                    string `json:"md5"`
	RUN_NUM                int64  `json:"run_num"`
}

// detailed files API response with runs and no EventCount
type fileDetailRunResponse struct {
	ADLER32            string  `json:"adler32" validate:"required"`
	AUTO_CROSS_SECTION float64 `json:"auto_cross_section"`
	BLOCK_ID           int64   `json:"block_id" validate:"number,gt=0"`
	BLOCK_NAME         string  `json:"block_name"`
	// BRANCH_HASH_ID         int64   `json:"branch_hash_id"`
	CHECK_SUM              string `json:"check_sum" validate:"required"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number,gt=0"`
	DATASET                string `json:"dataset"`
	DATASET_ID             int64  `json:"dataset_id" validate:"number,gt=0"`
	FILE_ID                int64  `json:"file_id"`
	FILE_SIZE              int64  `json:"file_size" validate:"required,number,gt=0"`
	FILE_TYPE              string `json:"file_type"`
	FILE_TYPE_ID           int64  `json:"file_type_id" validate:"number,gt=0"`
	IS_FILE_VALID          int64  `json:"is_file_valid" validate:"number"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
	LOGICAL_FILE_NAME      string `json:"logical_file_name" validate:"required"`
	MD5                    string `json:"md5"`
	RUN_NUM                int64  `json:"run_num"`
}

// creates a FileRecord depending on an integer
func createFileRecord(i int, dataset string, blockName string, fileLumiList []dbs.FileLumi, logicalFileName string, fileParentList []dbs.FileParentLFNRecord) dbs.FileRecord {
	return dbs.FileRecord{
		LOGICAL_FILE_NAME:  logicalFileName,
		IS_FILE_VALID:      1,
		DATASET:            dataset,
		BLOCK_NAME:         blockName,
		FILE_TYPE:          "EDM",
		CHECK_SUM:          "1504266448",
		EVENT_COUNT:        1619,
		ADLER32:            "NOTSET",
		AUTO_CROSS_SECTION: 0.0,
		CREATE_BY:          TestData.CreateBy,
		FILE_SIZE:          2012211901,
		FILE_LUMI_LIST:     fileLumiList,
		FILE_PARENT_LIST:   fileParentList,
		FILE_OUTPUT_CONFIG_LIST: []dbs.OutputConfigRecord{
			{
				RELEASE_VERSION:     TestData.ReleaseVersion,
				PSET_HASH:           TestData.PsetHash,
				APP_NAME:            TestData.AppName,
				OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
				GLOBAL_TAG:          TestData.GlobalTag,
			},
		},
		LAST_MODIFIED_BY: TestData.CreateBy,
	}
}

// create a detailed file response
func createFileDetailedResponse(i int, blockID int64, datasetID int64, fileRecord dbs.FileRecord) fileDetailResponse {
	return fileDetailResponse{
		ADLER32:                fileRecord.ADLER32,
		AUTO_CROSS_SECTION:     fileRecord.AUTO_CROSS_SECTION,
		BLOCK_ID:               blockID,
		BLOCK_NAME:             fileRecord.BLOCK_NAME,
		CHECK_SUM:              "1504266448",
		CREATE_BY:              TestData.CreateBy,
		CREATION_DATE:          0,
		DATASET:                fileRecord.DATASET,
		DATASET_ID:             datasetID,
		EventCount:             fileRecord.EVENT_COUNT,
		FILE_ID:                int64(i),
		FILE_SIZE:              fileRecord.FILE_SIZE,
		FILE_TYPE:              fileRecord.FILE_TYPE,
		FILE_TYPE_ID:           1,
		IS_FILE_VALID:          1,
		LAST_MODIFICATION_DATE: 0,
		LAST_MODIFIED_BY:       TestData.CreateBy,
		LOGICAL_FILE_NAME:      fileRecord.LOGICAL_FILE_NAME,
		MD5:                    "",
	}
}

// creates the parent file lumi list
func createParentFileLumiList() []dbs.FileLumi {
	return []dbs.FileLumi{
		{LumiSectionNumber: 27414, RunNumber: 97, EventCount: 66},
		{LumiSectionNumber: 26422, RunNumber: 97, EventCount: 67},
		{LumiSectionNumber: 29838, RunNumber: 97, EventCount: 68},
		{LumiSectionNumber: 248, RunNumber: 97, EventCount: 69},
		{LumiSectionNumber: 250, RunNumber: 97, EventCount: 70},
		{LumiSectionNumber: 300, RunNumber: 97, EventCount: 71},
		{LumiSectionNumber: 534, RunNumber: 97, EventCount: 72},
		{LumiSectionNumber: 546, RunNumber: 97, EventCount: 73},
		{LumiSectionNumber: 638, RunNumber: 97, EventCount: 74},
		{LumiSectionNumber: 650, RunNumber: 97, EventCount: 75},
		{LumiSectionNumber: 794, RunNumber: 97, EventCount: 76},
		{LumiSectionNumber: 1313, RunNumber: 97, EventCount: 77},
		{LumiSectionNumber: 1327, RunNumber: 97, EventCount: 78},
		{LumiSectionNumber: 1339, RunNumber: 97, EventCount: 79},
		{LumiSectionNumber: 1353, RunNumber: 97, EventCount: 80},
		{LumiSectionNumber: 1428, RunNumber: 97, EventCount: 81},
		{LumiSectionNumber: 1496, RunNumber: 97, EventCount: 82},
		{LumiSectionNumber: 1537, RunNumber: 97, EventCount: 83},
		{LumiSectionNumber: 1652, RunNumber: 97, EventCount: 84},
		{LumiSectionNumber: 1664, RunNumber: 97, EventCount: 85},
		{LumiSectionNumber: 1743, RunNumber: 97, EventCount: 86},
		{LumiSectionNumber: 1755, RunNumber: 97, EventCount: 87},
		{LumiSectionNumber: 1860, RunNumber: 97, EventCount: 88},
		{LumiSectionNumber: 1872, RunNumber: 97, EventCount: 89},
	}
}

// files endpoint tests
// TODO: handle BRANCH_HASH_ID
// TODO: Test with a request that does not contain is_file_valid
func getFilesTestTable(t *testing.T) EndpointTestCase {
	parentFileLumiList := createParentFileLumiList()
	var parentFiles []dbs.FileRecord
	var testDataParentFiles []string
	var parentDetailResp []Response
	for i := 1; i <= 10; i++ {
		parentLFN := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%v/%v.root", TestData.UID, i)
		testDataParentFiles = append(testDataParentFiles, parentLFN)
		fileRecord := createFileRecord(i, TestData.ParentDataset, TestData.ParentBlock, parentFileLumiList, parentLFN, []dbs.FileParentLFNRecord{})
		parentFiles = append(parentFiles, fileRecord)
		parentDetailResp = append(parentDetailResp, createFileDetailedResponse(i, 2, 2, fileRecord))
	}

	TestData.ParentFiles = testDataParentFiles

	fileLumiList := []dbs.FileLumi{
		{LumiSectionNumber: 27414, RunNumber: 97},
		{LumiSectionNumber: 26422, RunNumber: 98},
		{LumiSectionNumber: 29838, RunNumber: 99},
	}

	var files []dbs.FileRecord
	var lfns []Response
	var detailResp []Response
	var testDataFiles []string
	for i := 1; i <= 10; i++ {
		lfn := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%v/%v.root", TestData.UID, i)
		lfns = append(lfns, fileResponse{LOGICAL_FILE_NAME: lfn})
		testDataFiles = append(testDataFiles, lfn)
		fileParentLFN := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%v/%v.root", TestData.UID, i)
		fileParentList := []dbs.FileParentLFNRecord{
			{
				FILE_PARENT_LFN: fileParentLFN,
			},
		}
		fileRecord := createFileRecord(i, TestData.Dataset, TestData.Block, fileLumiList, lfn, fileParentList)
		files = append(files, fileRecord)
		detailResp = append(detailResp, createFileDetailedResponse(i+10, 1, 1, fileRecord))
	}

	TestData.Files = testDataFiles

	// add run_num
	var fileRunResp []Response
	for _, lfn := range lfns {
		frr := fileRunResponse{
			LOGICAL_FILE_NAME: lfn.(fileResponse).LOGICAL_FILE_NAME,
			RUN_NUM:           99,
		}
		fileRunResp = append(fileRunResp, frr)
	}

	return EndpointTestCase{
		description:     "Test files",
		defaultHandler:  web.FilesHandler,
		defaultEndpoint: "/dbs/files",
		testCases: []testCase{
			{
				description: "Test parent file POST", // DBSClientWriter_t.test16
				method:      "POST",
				serverType:  "DBSWriter",
				input: dbs.PyFileRecord{
					Records: parentFiles,
				},
				params: url.Values{
					"dataset": []string{TestData.ParentDataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test file POST", // DBSClientWriter_t.test17
				method:      "POST",
				serverType:  "DBSWriter",
				input: dbs.PyFileRecord{
					Records: files,
				},
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test file duplicate POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: dbs.PyFileRecord{
					Records: files,
				},
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset", // DBSClientReader_t.test032
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   lfns,
				respCode: http.StatusOK,
			},
			{
				description: "Test detail GET",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
					"detail":  []string{"true"},
				},
				output:   detailResp,
				respCode: http.StatusOK,
			},
			{
				description: "Test detail parent GET",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.ParentDataset},
					"detail":  []string{"true"},
				},
				output:   parentDetailResp,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset run_num and lumi_list params",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":   []string{TestData.Dataset},
					"run_num":   []string{"99"},
					"lumi_list": []string{"[29838]"},
				},
				output:   fileRunResp,
				respCode: http.StatusOK,
			},
		},
	}
}

// files PUT request body struct
type filesPUTRequest struct {
	LOGICAL_FILE_NAME string `json:"logical_file_name"`
	IS_FILE_VALID     int64  `json:"is_file_valid" validate:"number"`
}

// files endpoint update tests
func getFilesTestTable2(t *testing.T) EndpointTestCase {
	var parentLFNs []Response
	var testDataParentFiles []string
	for i := 1; i <= 10; i++ {
		parentLFN := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%v/%v.root", TestData.UID, i)
		parentLFNs = append(parentLFNs, fileResponse{LOGICAL_FILE_NAME: parentLFN})
		testDataParentFiles = append(testDataParentFiles, parentLFN)
	}

	TestData.ParentFiles = testDataParentFiles

	fileLumiList := []dbs.FileLumi{
		{LumiSectionNumber: 27414, RunNumber: 97},
		{LumiSectionNumber: 26422, RunNumber: 98},
		{LumiSectionNumber: 29838, RunNumber: 99},
	}

	var lfns []Response
	var detailResp []Response
	var detailResp2 []Response // for testing GET after PUT
	var testDataFiles []string
	for i := 1; i <= 10; i++ {
		lfn := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%v/%v.root", TestData.UID, i)
		lfns = append(lfns, fileResponse{LOGICAL_FILE_NAME: lfn})
		testDataFiles = append(testDataFiles, lfn)
		fileParentLFN := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%v/%v.root", TestData.UID, i)
		fileParentList := []dbs.FileParentLFNRecord{
			{
				FILE_PARENT_LFN: fileParentLFN,
			},
		}
		fileRecord := createFileRecord(i, TestData.Dataset, TestData.Block, fileLumiList, lfn, fileParentList)
		singleDetailResp := createFileDetailedResponse(i+10, 1, 1, fileRecord)
		singleDetailResp2 := singleDetailResp
		if i == 1 {
			singleDetailResp2.IS_FILE_VALID = 0
			singleDetailResp2.LAST_MODIFIED_BY = "DBS-workflow"
		}
		detailResp = append(detailResp, singleDetailResp)
		detailResp2 = append(detailResp2, singleDetailResp2)
	}

	TestData.Files = testDataFiles
	lfn := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%v/%v.root", TestData.UID, 1)
	fileReq := filesPUTRequest{
		LOGICAL_FILE_NAME: lfn,
		IS_FILE_VALID:     0,
	}

	fileResp := fileDetailResponse{
		ADLER32:                "NOTSET",
		AUTO_CROSS_SECTION:     0.0,
		BLOCK_ID:               1,
		BLOCK_NAME:             TestData.Block,
		CHECK_SUM:              "1504266448",
		CREATE_BY:              TestData.CreateBy,
		CREATION_DATE:          0,
		DATASET:                TestData.Dataset,
		DATASET_ID:             1,
		EventCount:             1619,
		FILE_ID:                11,
		FILE_SIZE:              2.012211901e+09,
		FILE_TYPE:              "EDM",
		FILE_TYPE_ID:           1,
		IS_FILE_VALID:          1,
		LAST_MODIFICATION_DATE: 0,
		LAST_MODIFIED_BY:       TestData.CreateBy,
		LOGICAL_FILE_NAME:      lfn,
		MD5:                    "",
	}

	fileResp2 := fileResp
	fileResp2.IS_FILE_VALID = 0
	fileResp2.LAST_MODIFIED_BY = "DBS-workflow"

	// add run_num
	var fileRunDetailResp []Response
	for _, fd := range detailResp2 {
		for i := 0; i < 9; i++ {
			var frr fileDetailRunResponse
			fr, err := json.Marshal(fd)
			if err != nil {
				t.Fatal(err)
			}
			err = json.Unmarshal(fr, &frr)
			if err != nil {
				t.Fatal(err)
			}
			frr.RUN_NUM = 97
			fileRunDetailResp = append(fileRunDetailResp, frr)
		}
		var frr fileDetailRunEventResponse
		fr, err := json.Marshal(fd)
		if err != nil {
			t.Fatal(err)
		}
		err = json.Unmarshal(fr, &frr)
		if err != nil {
			t.Fatal(err)
		}
		frr.RUN_NUM = 97
		frr.EventCount = 1619
	}

	// filtered detailed response

	return EndpointTestCase{
		description:     "Test files update",
		defaultHandler:  web.FilesHandler,
		defaultEndpoint: "/dbs/files",
		testCases: []testCase{
			{
				description: "Test GET before update",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{lfn},
					"detail":            []string{"true"},
				},
				output: []Response{
					fileResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test update file status",
				method:      "PUT",
				serverType:  "DBSWriter",
				input:       fileReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after update",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{lfn},
					"detail":            []string{"true"},
				},
				output: []Response{
					fileResp2,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset validFileOnly true", // DBSClientReader_t.test032a
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":       []string{TestData.Dataset},
					"validFileOnly": []string{"1"},
				},
				output:   lfns[1:],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset validFileOnly false", // DBSClientReader_t.test032b
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":       []string{TestData.Dataset},
					"validFileOnly": []string{"0"},
				},
				output:   lfns,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset, validFileOnly true, detail, sumOverLumi", // DBSClientReader_t.test032c
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":       []string{TestData.Dataset},
					"validFileOnly": []string{"1"},
					"detail":        []string{"1"},
					"sumOverLumi":   []string{"1"},
				},
				output:   detailResp[1:],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset, validFileOnly true, detail, sumOverLumi, runs", // DBSClientReader_t.test032d
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset":       []string{TestData.Dataset},
					"validFileOnly": []string{"1"},
					"detail":        []string{"1"},
					"sumOverLumi":   []string{"1"},
					"run_num":       []string{fmt.Sprint(TestData.Runs[0])},
				},
				output:   fileRunDetailResp[9:],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET validFileOnly", // DBSClientReader_t.test033
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"validFileOnly": []string{"1"},
				},
				output:   append(parentLFNs, lfns[1:]...),
				respCode: http.StatusOK,
			},
			{
				description: "Test GET validFileOnly false", // DBSClientReader_t.test034
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"validFileOnly": []string{"0"},
				},
				output:   append(parentLFNs, lfns...),
				respCode: http.StatusOK,
			},
		},
	}
}

// test files with lumi_list range
func getFilesLumiListRangeTestTable(t *testing.T) EndpointTestCase {
	// filtered detailed response
	childBulk := BulkBlocksData.ConcurrentChildData
	var lfns []Response
	var detailResp3 []Response
	var detailRunResp []Response
	var fileRunResp []Response
	for i, v := range childBulk.Files {
		r := fileResponse{
			LOGICAL_FILE_NAME: v.LogicalFileName,
		}
		lfns = append(lfns, r)
		detail := fileDetailResponse{
			ADLER32:                v.Adler32,
			AUTO_CROSS_SECTION:     v.AutoCrossSection,
			BLOCK_ID:               4,
			BLOCK_NAME:             childBulk.Block.BlockName,
			CHECK_SUM:              v.CheckSum,
			CREATE_BY:              "DBS-workflow",
			CREATION_DATE:          childBulk.Dataset.CreationDate,
			DATASET:                childBulk.Dataset.Dataset,
			DATASET_ID:             5,
			EventCount:             v.EventCount,
			FILE_ID:                int64(26 + i),
			FILE_SIZE:              v.FileSize,
			FILE_TYPE:              v.FileType,
			FILE_TYPE_ID:           1,
			IS_FILE_VALID:          v.IsFileValid,
			LAST_MODIFICATION_DATE: 1652196887,
			LAST_MODIFIED_BY:       "DBS-workflow",
			LOGICAL_FILE_NAME:      v.LogicalFileName,
			MD5:                    "",
		}
		var detailRun fileDetailRunEventResponse
		d, err := json.Marshal(detail)
		if err != nil {
			t.Fatal(err.Error())
		}
		err = json.Unmarshal(d, &detailRun)
		if err != nil {
			t.Fatal(err.Error())
		}
		detailRun.RUN_NUM = 98
		detailRun.EventCount = 201
		detailRunResp = append(detailRunResp, detailRun)

		detailResp3 = append(detailResp3, detail)
		fileRunResp = append(fileRunResp, fileRunResponse{
			LOGICAL_FILE_NAME: v.LogicalFileName,
			RUN_NUM:           v.FileLumiList[0].RunNumber,
		})
	}

	runNumParam := fmt.Sprint(childBulk.Files[0].FileLumiList[0].RunNumber)
	// runNumParam2 := fmt.Sprint(childBulk.Files[0].FileLumiList[2].RunNumber)

	dbsErrorNest := dbs.DBSError{
		Reason:   "near \"AS\": syntax error",
		Code:     dbs.QueryErrorCode,
		Message:  "",
		Function: "dbs.executeAll",
	}
	dbsError := dbs.DBSError{
		Function: "dbs.files.Files",
		Code:     dbs.QueryErrorCode,
		Reason:   dbsErrorNest.Error(),
		Message:  "",
	}
	hrec := web.HTTPError{
		Method:    "GET",
		Timestamp: "",
		HTTPCode:  http.StatusBadRequest,
		Path:      "/dbs/files?block_name=/unittest_web_primary_ds_name_8268_stepchain/acq_era_8268-v8268/GEN-SIM-RAW#8268&detail=true&lumi_list=[27414,26422,29838]&run_num=98&sumOverLumi=1",
		UserAgent: "Go-http-client/1.1",
	}
	errorResp := web.ServerError{
		HTTPError: hrec,
		DBSError:  &dbsError,
		Exception: http.StatusBadRequest,
		Type:      "HTTPError",
		Message:   dbsError.Error(),
	}

	dbsError2 := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Code:     dbs.ParametersErrorCode,
		Message:  "When sumOverLumi=1, no run_num list is allowed",
		Function: "dbs.files.Files",
	}
	hrec2 := web.HTTPError{
		Method:    "GET",
		Timestamp: "",
		HTTPCode:  http.StatusBadRequest,
		Path:      "/dbs/files?block_name=/unittest_web_primary_ds_name_8268_stepchain/acq_era_8268-v8268/GEN-SIM-RAW#8268&detail=true&lumi_list=[27414,26422,29838]&run_num=98&sumOverLumi=1",
		UserAgent: "Go-http-client/1.1",
	}
	errorResp2 := web.ServerError{
		HTTPError: hrec2,
		DBSError:  &dbsError2,
		Exception: http.StatusBadRequest,
		Type:      "HTTPError",
		Message:   dbsError2.Error(),
	}

	var largeFileResp []Response
	err := readJsonFile(t, "./data/integration/files_response_data.json", &largeFileResp)
	if err != nil {
		t.Fatal(err.Error())
	}

	var largeFileResp2 []Response
	err = readJsonFile(t, "./data/integration/files_response_data2.json", &largeFileResp2)
	if err != nil {
		t.Fatal(err.Error())
	}

	return EndpointTestCase{
		description:     "Test files with lumi_list ranges",
		defaultHandler:  web.FilesHandler,
		defaultEndpoint: "/dbs/files",
		testCases: []testCase{
			{
				description: "Test GET with block_name", // DBSClientReader_t.test033a
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
				},
				output:   lfns,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, detail", // DBSClientReader_t.test033b
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
					"detail":     []string{"true"},
				},
				output:   detailResp3,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name and validFileOnly", // DBSClientReader_t.test033c
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"validFileOnly": []string{"1"},
				},
				output:   lfns,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, validFileOnly, detail", // DBSClientReader_t.test033d
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"validFileOnly": []string{"1"},
					"detail":        []string{"true"},
				},
				output:   detailResp3,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name and validFileOnly false", // DBSClientReader_t.test033e
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"validFileOnly": []string{"0"},
				},
				output:   lfns,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, validFileOnly false, detail", // DBSClientReader_t.test033f
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"validFileOnly": []string{"0"},
					"detail":        []string{"true"},
				},
				output:   detailResp3,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, lumi_list", // DBSClientReader_t.test033g
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
					"run_num":    []string{runNumParam},
					"lumi_list":  []string{"[27414,26422,29838]"},
				},
				output:   fileRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, nested lumi_list", // DBSClientReader_t.test033h
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
					"run_num":    []string{runNumParam},
					"lumi_list":  []string{"[[27414 27418] [26422 26426] [29838 29842]]"},
				},
				output:   fileRunResp,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, lumi_list, detail", // DBSClientReader_t.test033i
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
					"run_num":    []string{runNumParam},
					"lumi_list":  []string{"[27414,26422,29838]"},
					"detail":     []string{"true"},
				},
				output:   detailRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, nested lumi_list, detail", // DBSClientReader_t.test033j
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
					"run_num":    []string{runNumParam},
					"lumi_list":  []string{"[[27414 27418] [26422 26426] [29838 29842]]"},
					"detail":     []string{"true"},
				},
				output:   detailRunResp,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, lumi_list, validFileOnly", // DBSClientReader_t.test033k
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"run_num":       []string{runNumParam},
					"lumi_list":     []string{"[27414,26422,29838]"},
					"validFileOnly": []string{"1"},
				},
				output:   fileRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, nested lumi_list, validFileOnly", // DBSClientReader_t.test033l
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"run_num":       []string{runNumParam},
					"lumi_list":     []string{"[[27414 27418] [26422 26426] [29838 29842]]"},
					"validFileOnly": []string{"1"},
				},
				output:   fileRunResp,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, lumi_list, detail, validFileOnly", // DBSClientReader_t.test033m
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"run_num":       []string{runNumParam},
					"lumi_list":     []string{"[27414,26422,29838]"},
					"validFileOnly": []string{"1"},
					"detail":        []string{"true"},
				},
				output:   detailRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, nested lumi_list, detail, validFileOnly", // DBSClientReader_t.test033n
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.StepchainBlock},
					"run_num":       []string{runNumParam},
					"lumi_list":     []string{"[[27414 27418] [26422 26426] [29838 29842]]"},
					"validFileOnly": []string{"1"},
					"detail":        []string{"true"},
				},
				output:   detailRunResp,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, run_num, lumi_list", // DBSClientReader_t.test033o
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{TestData.StepchainBlock},
					"run_num":    []string{fmt.Sprintf("[%s]", runNumParam)},
					"lumi_list":  []string{"[27414,26422,29838]"},
				},
				output:   fileRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test bad GET with block_name, sumOverLumi, run_num, lumi_list, detail", // DBSClientReader_t.test033p
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":  []string{TestData.StepchainBlock},
					"sumOverLumi": []string{"1"},
					"run_num":     []string{runNumParam},
					"lumi_list":   []string{"[27414,26422,29838]"},
					"detail":      []string{"true"},
				},
				output:   []Response{errorResp},
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test bad GET with block_name, sumOverLumi, single run_num, detail", // DBSClientReader_t.test033q
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":  []string{TestData.StepchainBlock},
					"sumOverLumi": []string{"1"},
					"run_num":     []string{fmt.Sprintf("[%s]", runNumParam)},
					"detail":      []string{"true"},
				},
				output:   []Response{errorResp2},
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test GET with block_name, sumOverLumi, run_num, detail", // DBSClientReader_t.test033r
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":  []string{TestData.Block},
					"sumOverLumi": []string{"1"},
					"run_num":     []string{"97-99"},
					"detail":      []string{"true"},
				},
				output:   largeFileResp,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with block_name, sumOverLumi, validFileOnly, run_num, detail", // DBSClientReader_t.test033s
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name":    []string{TestData.Block},
					"sumOverLumi":   []string{"1"},
					"validFileOnly": []string{"1"},
					"run_num":       []string{"97-99"},
					"detail":        []string{"true"},
				},
				output:   largeFileResp2,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn", // DBSClientReader_t.test034a
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
				},
				output:   lfns[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn, validFileOnly", // DBSClientReader_t.test034b
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
					"validFileOnly":     []string{"1"},
				},
				output:   lfns[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn, validFileOnly false", // DBSClientReader_t.test034c
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
					"validFileOnly":     []string{"0"},
				},
				output:   lfns[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn, run_num, lumi_list", // DBSClientReader_t.test034d
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
					"run_num":           []string{runNumParam},
					"lumi_list":         []string{"[27414,26422,29838]"},
				},
				output:   fileRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn, run_num, nested lumi_list", // DBSClientReader_t.test034e
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
					"run_num":           []string{runNumParam},
					"lumi_list":         []string{"[[27414 27418] [26422 26426] [29838 29842]]"},
				},
				output:   fileRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn, run_num, lumi_list, detail", // DBSClientReader_t.test034f
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
					"run_num":           []string{runNumParam},
					"lumi_list":         []string{"[27414,26422,29838]"},
					"detail":            []string{"true"},
				},
				output:   detailRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn, run_num, nested lumi_list, detail", // DBSClientReader_t.test034g
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
					"run_num":           []string{runNumParam},
					"lumi_list":         []string{"[[27414 27418] [26422 26426] [29838 29842]]"},
					"detail":            []string{"true"},
				},
				output:   detailRunResp[:1],
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with lfn, run_num, lumi_list, validFileOnly", // DBSClientReader_t.test034h
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{childBulk.Files[0].LogicalFileName},
					"run_num":           []string{runNumParam},
					"lumi_list":         []string{"[27414,26422,29838]"},
					"validFileOnly":     []string{"1"},
				},
				output:   fileRunResp[:1],
				respCode: http.StatusOK,
			},
		},
	}
}
