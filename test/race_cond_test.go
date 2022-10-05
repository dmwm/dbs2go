package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/dmwm/dbs2go/dbs"
)

var testData initialData

// creates a file for bulkblocks
func createFile2(t *testing.T, i int, factor int) dbs.File {
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
		LogicalFileName: fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/StepChain_/p%v%d/%v%d.root", TestData.UID, i*10, i, factor*100),
		IsFileValid:     1,
	}
}

// generate a single bulkblock
func generateBulkBlock(t *testing.T, factor int) dbs.BulkBlocks {
	var bulkBlock dbs.BulkBlocks

	algo := dbs.DatasetConfig{
		ReleaseVersion:    testData.ReleaseVersion,
		PsetHash:          testData.PsetHash,
		AppName:           testData.AppName,
		OutputModuleLabel: testData.OutputModuleLabel,
		GlobalTag:         testData.GlobalTag,
	}

	primDS := dbs.PrimaryDataset{
		PrimaryDSName: testData.StepPrimaryDSName,
		PrimaryDSType: "test",
		CreateBy:      "WMAgent",
		CreationDate:  time.Now().Unix(), // Replace with fixed time
	}

	dataset := dbs.Dataset{
		PhysicsGroupName:     testData.PhysicsGroupName,
		ProcessedDSName:      testData.ProcDataset,
		DataTierName:         testData.Tier,
		DatasetAccessType:    testData.DatasetAccessType2,
		Dataset:              testData.StepchainDataset,
		PrepID:               "TestPrepID",
		CreateBy:             "WMAgent",
		LastModifiedBy:       "WMAgent",
		CreationDate:         time.Now().Unix(),
		LastModificationDate: time.Now().Unix(),
	}

	processingEra := dbs.ProcessingEra{
		ProcessingVersion: testData.ProcessingVersion,
		CreateBy:          "WMAgent",
	}

	acqEra := dbs.AcquisitionEra{
		AcquisitionEraName: testData.AcquisitionEra,
		StartDate:          123456789,
	}

	fileCount := 100

	block := dbs.Block{
		BlockName:      fmt.Sprintf("%s%d", testData.StepchainBlock, factor),
		OriginSiteName: testData.Site,
		FileCount:      int64(fileCount),
		BlockSize:      20122119010,
	}

	bulkBlock.DatasetConfigList = []dbs.DatasetConfig{algo}
	bulkBlock.PrimaryDataset = primDS
	bulkBlock.Dataset = dataset
	bulkBlock.ProcessingEra = processingEra
	bulkBlock.AcquisitionEra = acqEra
	// bulkBlock.DatasetParentList = []string{testData.ParentStepchainDataset}
	bulkBlock.Block = block

	for i := 0; i < fileCount; i++ {
		f := createFile2(t, i*factor, factor)
		bulkBlock.Files = append(bulkBlock.Files, f)
	}

	return bulkBlock
}

func TestRaceConditions(t *testing.T) {
	t.Log("Starting tests for race conditions")
	// var BulkBlocksData dbs.BulkBlocks

	t.Cleanup(func() {
		exec.Command("pkill", "dbs2go").Output()
	})

	// load basic data definitions
	testCaseFile := os.Getenv("INTEGRATION_DATA_FILE")
	if testCaseFile == "" {
		t.Fatal("INTEGRATION_DATA_FILE not defined")
	}
	if _, err := os.Stat(testCaseFile); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Generating base data")
		generateBaseData(t, testCaseFile)
	}
	err := readJsonFile(t, testCaseFile, &testData)
	if err != nil {
		t.Fatal(err.Error())
	}

	numBlocks := 1000
	var blocks []dbs.BulkBlocks

	for i := 1; i < numBlocks+1; i++ {
		b := generateBulkBlock(t, i)
		blocks = append(blocks, b)
	}

	// time.Sleep(time.Minute)

	var wg sync.WaitGroup

	t.Run("Insert blocks simultaneously", func(t *testing.T) {
		for _, block := range blocks {
			// time.Sleep(1 * time.Millisecond)
			wg.Add(1)
			go func(t *testing.T, b dbs.BulkBlocks) {
				defer wg.Done()
				data, err := json.Marshal(b)
				if err != nil {
					t.Error(err)
				}
				reader := bytes.NewReader(data)
				ht := http.DefaultTransport.(*http.Transport).Clone()
				ht.CloseIdleConnections()
				ht.DisableKeepAlives = true
				ht.MaxIdleConnsPerHost = 1
				ht.MaxIdleConns = 1
				c := &http.Client{Transport: ht}

				resp, err := c.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
				if err != nil {
					t.Error(err)
				}
				if resp.StatusCode != http.StatusOK {
					t.Logf("%v\n", resp.StatusCode)
				}
			}(t, block)
		}
		wg.Wait()
		fmt.Println("Done")
	})

}
