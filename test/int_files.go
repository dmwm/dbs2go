package main

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// this file contains logic for files API

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

func createDetailedResponse(i int, blockID int64, datasetID int64, fileRecord dbs.FileRecord) fileDetailResponse {
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
		IS_FILE_VALID:          0,
		LAST_MODIFICATION_DATE: 0,
		LAST_MODIFIED_BY:       TestData.CreateBy,
		LOGICAL_FILE_NAME:      fileRecord.LOGICAL_FILE_NAME,
		MD5:                    "",
	}
}

// files endpoint tests
// TODO: handle BRANCH_HASH_ID
func getFilesTestTable(t *testing.T) EndpointTestCase {
	parentFileLumiList := []dbs.FileLumi{
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
	var parentFiles []dbs.FileRecord
	var parentLFNs []Response
	var parentDetailResp []Response
	for i := 1; i <= 10; i++ {
		parentLFN := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%v/%v.root", TestData.UID, i)
		parentLFNs = append(parentLFNs, fileResponse{LOGICAL_FILE_NAME: parentLFN})
		fileRecord := createFileRecord(i, TestData.ParentDataset, TestData.ParentBlock, parentFileLumiList, parentLFN, []dbs.FileParentLFNRecord{})
		parentFiles = append(parentFiles, fileRecord)
		parentDetailResp = append(parentDetailResp, createDetailedResponse(i, 2, 2, fileRecord))
	}

	fileLumiList := []dbs.FileLumi{
		{LumiSectionNumber: 27414, RunNumber: 97},
		{LumiSectionNumber: 26422, RunNumber: 98},
		{LumiSectionNumber: 29838, RunNumber: 99},
	}

	var files []dbs.FileRecord
	var lfns []Response
	var detailResp []Response
	for i := 1; i <= 10; i++ {
		lfn := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%v/%v.root", TestData.UID, i)
		lfns = append(lfns, fileResponse{LOGICAL_FILE_NAME: lfn})
		fileParentLFN := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%v/%v.root", TestData.UID, i)
		fileParentList := []dbs.FileParentLFNRecord{
			{
				FILE_PARENT_LFN: fileParentLFN,
			},
		}
		fileRecord := createFileRecord(i, TestData.Dataset, TestData.Block, fileLumiList, lfn, fileParentList)
		files = append(files, fileRecord)
		detailResp = append(detailResp, createDetailedResponse(i+10, 1, 1, fileRecord))
	}

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
				description: "Test parent file POST",
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
				description: "Test file POST",
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
				description: "Test GET",
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
