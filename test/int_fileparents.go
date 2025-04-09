package main

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// response for fileparent
type fileParentResponse struct {
	LOGICAL_FILE_NAME        string `json:"logical_file_name"`
	PARENT_FILE_ID           int    `json:"parent_file_id"`
	PARENT_LOGICAL_FILE_NAME string `json:"parent_logical_file_name"`
}

func getFileParentsTestTable(t *testing.T) EndpointTestCase {
	fpResp := fileParentResponse{
		LOGICAL_FILE_NAME:        TestData.Files[0],
		PARENT_FILE_ID:           1,
		PARENT_LOGICAL_FILE_NAME: TestData.ParentFiles[0],
	}
	dbsError := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Message:  "logical_file_name, block_id or block_name is required for fileparents api",
		Code:     dbs.InvalidParameterErrorCode,
		Function: "dbs.fileparents.FileParents",
	}
	hrec := createHTTPError("GET", "/dbs/primarydstypes?dataset=fnal")
	errorResp := web.ServerError{
		HTTPError: hrec,
		DBSError:  &dbsError,
		Exception: http.StatusBadRequest,
		Type:      "HTTPError",
		Message:   dbsError.Error(),
	}

	// insert fileparent record requests
	partialChildParentIDList := [][]int64{
		{21, 25},
		{22, -1},
		{23, -1},
		{24, -1},
		{25, -1},
	}
	fpBlockPartialRecordGood := dbs.FileParentBlockRecord{
		BlockName:         BulkBlocksData.SequentialParentData.Block.BlockName,
		ChildParentIDList: partialChildParentIDList,
		MissingFiles:      4,
	}
	fpBlockPartialRecordBad := dbs.FileParentBlockRecord{
		BlockName:         BulkBlocksData.SequentialParentData.Block.BlockName,
		ChildParentIDList: partialChildParentIDList,
		MissingFiles:      3,
	}

	completeChildParentIDList := [][]int64{
		{21, 1},
		{22, 2},
		{23, 3},
		{24, 4},
		{25, 5},
	}
	fpBlockCompleteRecordGood := dbs.FileParentBlockRecord{
		BlockName:         BulkBlocksData.SequentialParentData.Block.BlockName,
		ChildParentIDList: completeChildParentIDList,
		MissingFiles:      0,
	}

	// responses for fileparents
	// logical_file_name:/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/StepChain_ptr/p8268/5.root
	// parent_file_id:1
	// parent_logical_file_name:/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p8268/1.root
	var fpRespList []Response
	for i := 1; i <= 5; i++ {
		lfn := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%v/%v.root", TestData.UID, i)
		fpR := fileParentResponse{
			LOGICAL_FILE_NAME:        BulkBlocksData.SequentialChildData.Files[i-1].LogicalFileName,
			PARENT_FILE_ID:           i,
			PARENT_LOGICAL_FILE_NAME: lfn,
		}
		fpRespList = append(fpRespList, fpR)
	}
	fpR := fileParentResponse{
		LOGICAL_FILE_NAME:        BulkBlocksData.SequentialChildData.Files[0].LogicalFileName,
		PARENT_FILE_ID:           25,
		PARENT_LOGICAL_FILE_NAME: BulkBlocksData.SequentialParentData.Files[4].LogicalFileName,
	}
	fpRespList = append(fpRespList[:2], fpRespList[1:]...)
	fpRespList[1] = fpR

	return EndpointTestCase{
		description:     "Test fileparents",
		defaultHandler:  web.FileParentsHandler,
		defaultEndpoint: "/dbs/fileparents",
		testCases: []testCase{
			{ // DBSClientReader_t.test041
				description: "Test fileparents with parent lfn",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{TestData.ParentFiles[0]},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{ // DBSClientReader_t.test041
				description: "Test fileparents with file lfn",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"logical_file_name": []string{TestData.Files[0]},
				},
				output:   []Response{fpResp},
				respCode: http.StatusOK,
			},
			{ // DBSClientReader_t.test042
				description: "Test fileparents with no params",
				method:      "GET",
				serverType:  "DBSReader",
				params:      url.Values{},
				output:      []Response{errorResp},
				respCode:    http.StatusBadRequest,
			},
			{
				description: "Test fileparents insert with partial parentage and bad missingFiles",
				method:      "POST",
				serverType:  "DBSWriter",
				params: url.Values{
					"block_name": []string{BulkBlocksData.SequentialChildData.Block.BlockName},
				},
				input:    fpBlockPartialRecordBad,
				output:   []Response{},
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test fileparents insert with partial parentage and good missingFiles",
				method:      "POST",
				serverType:  "DBSWriter",
				params: url.Values{
					"block_name": []string{BulkBlocksData.SequentialChildData.Block.BlockName},
				},
				input:    fpBlockPartialRecordGood,
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test fileparents insert with complete parentage and good missingFiles",
				method:      "POST",
				serverType:  "DBSWriter",
				params: url.Values{
					"block_name": []string{BulkBlocksData.SequentialChildData.Block.BlockName},
				},
				input:    fpBlockCompleteRecordGood,
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET fileparents with parent block name",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"block_name": []string{BulkBlocksData.SequentialParentData.Block.BlockName},
				},
				output:   fpRespList,
				respCode: http.StatusOK,
			},
		},
	}
}
