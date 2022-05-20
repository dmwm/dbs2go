package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/utils"
	"github.com/dmwm/dbs2go/web"
)

// TestMigrateGetBlocks
func TestMigrateGetBlocks(t *testing.T) {
	rurl := "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
	if rurl == "" {
		return
	}
	//     parentDataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW"
	dataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM"
	blocks, err := dbs.GetBlocks(rurl, dataset)
	if err != nil {
		t.Error("Fail TestMigrateGetBlocks")
	}
	fmt.Printf("url=%s dataset=%s blocks=%v\n", rurl, dataset, blocks)
	if len(blocks) != 1 {
		t.Error("Wrong number of expected blocks")
	}
	blk := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM#e9b596e0-25b1-4c17-a628-9d9964be123a"
	if blocks[0] != blk {
		t.Error("Unexpected block")
	}
	blocks, err = dbs.GetBlocks(rurl, blk)
	if err != nil {
		t.Error("Fail TestMigrateGetBlocks")
	}
	fmt.Printf("url=%s block=%s blocks=%v\n", rurl, blk, blocks)
	if len(blocks) != 1 {
		t.Error("Wrong number of expected blocks")
	}
	if blocks[0] != blk {
		t.Error("Unexpected block")
	}
}

// TestMigrateGetParents
func TestMigrateGetParents(t *testing.T) {
	//     t.Error("Fail TestInList")
}

// TestMigrateGetParentBlocks
func TestMigrateGetParentBlocks(t *testing.T) {
	blk := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM#e9b596e0-25b1-4c17-a628-9d9964be123a"
	parents := []string{
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW#15f769b1-a371-4f5d-8d0f-d9c4a6723869",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW#53c10dee-274d-412a-82ca-6f925ac8ed72",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIFall18GS-SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM#a52529ca-c902-45c9-a372-0fadaf96a159",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIFall18GS-SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM#a52529ca-c902-45c9-a372-0fadaf96a159",
	}
	rurl := "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
	if rurl == "" {
		return
	}
	utils.Localhost = "http://localhost:9898"
	utils.VERBOSE = 2
	log.SetFlags(0)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	result, err := dbs.GetParentBlocks(rurl, blk)
	if err != nil {
		t.Error("unable to get parent blocks, error", err)
	}
	fmt.Println("expect", parents)
	fmt.Println("result", result)
	for _, blk := range parents {
		if !utils.InList(blk, result) {
			t.Error("block", blk, "not found in result list")
		}
	}
}

// TestMigrateGetParentDatasets
func TestMigrateGetParentDatasets(t *testing.T) {
	rurl := "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
	if rurl == "" {
		return
	}
	parentDataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW"
	dataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM"
	// GetParents finds immediate parent of the input (dataset)
	datasets, err := dbs.GetParents(rurl, dataset)
	if err != nil {
		t.Error("Fail TestMigrateGetParentDatasets", err)
	}
	if len(datasets) != 1 {
		t.Error("Wrong number of expected datasets")
	}
	if datasets[0] != parentDataset {
		t.Error("Unexpected dataset")
	}
}

// TestMigrateGetParentDatasetBlocks
func TestMigrateGetParentDatasetBlocks(t *testing.T) {
	rurl := "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
	if rurl == "" {
		return
	}
	parentBlocks := []string{
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW#15f769b1-a371-4f5d-8d0f-d9c4a6723869",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW#53c10dee-274d-412a-82ca-6f925ac8ed72",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIFall18GS-SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM#a52529ca-c902-45c9-a372-0fadaf96a159",
	}
	dataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM"
	// GetParentDatasetBlocks find full list of parent blocks
	pblocks, err := dbs.GetParentDatasetBlocks(rurl, dataset)
	if err != nil {
		t.Error("Fail TestMigrateGetParentDatasets", err)
	}
	log.Println("parent blocks", parentBlocks)
	log.Println("found parent blocks", pblocks)
	if len(pblocks) == 0 {
		t.Error("no parent blocks are found")
	}
	for _, blk := range pblocks {
		if !utils.InList(blk, parentBlocks) {
			t.Error("block", blk, "not found in parent blocks list")
		}
	}
}

// TestMigrate
//gocyclo:ignore
func TestMigrate(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()
	utils.VERBOSE = 1

	// setup HTTP request
	migFile := "data/mig_request.json"
	data, err := os.ReadFile(migFile)
	if err != nil {
		log.Printf("ERROR: unable to read %s error %v", migFile, err.Error())
		t.Fatal(err.Error())
	}
	reader := bytes.NewReader(data)

	// test existing DBS API
	rr, err := respRecorder("POST", "/dbs2go/submit", reader, web.MigrationSubmitHandler)
	if err != nil {
		t.Error(err)
	}

	// unmarshal received records
	var reports []dbs.MigrationReport
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &reports)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Println("Received data", string(data))
	var rids []int64
	for _, rrr := range reports {
		req := rrr.MigrationRequest
		if req.MIGRATION_STATUS != 0 {
			t.Errorf("invalid return status of migration request %+v", rrr)
		}
		rids = append(rids, req.MIGRATION_REQUEST_ID)
	}

	// now we should request status of the migration request
	rr, err = respRecorder("GET", "dbs2go/status", reader, web.MigrationStatusHandler)
	if err != nil {
		t.Error(err)
	}
	var statusRecords []dbs.MigrationRequest
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &statusRecords)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Println("Received data", string(data))
	var sids []int64
	for _, rrr := range statusRecords {
		sids = append(sids, rrr.MIGRATION_REQUEST_ID)
		if !utils.InList(rrr.MIGRATION_REQUEST_ID, rids) {
			t.Errorf("unvalid status request id %d, expect %+v", rrr.MIGRATION_REQUEST_ID, rids)
		}
	}
	if len(rids) != len(sids) {
		t.Errorf("wrong number of status IDs %+v, expect +%v", sids, rids)
	}

	// finally, let's process specific migration request
	dbs.MigrationProcessTimeout = 100
	procFile := "data/mig_request_process.json"
	data, err = os.ReadFile(procFile)
	if err != nil {
		log.Printf("ERROR: unable to read %s error %v", procFile, err.Error())
		t.Fatal(err.Error())
	}
	reader = bytes.NewReader(data)
	rr, err = respRecorder("POST", "dbs2go/process", reader, web.MigrationProcessHandler)
	if err != nil {
		t.Error(err)
	}
	var reportRecords []dbs.MigrationReport
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &reportRecords)
	if err != nil {
		t.Errorf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Println("Received data", string(data))
	for _, rec := range reportRecords {
		if rec.Status != "COMPLETED" {
			t.Errorf("invalid status in %+v, expected COMPLETED", rec)
		}
	}
}

// TestMigrateRemove tests remove migration APIs
//gocyclo:ignore
func TestMigrateRemove(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()
	utils.VERBOSE = 1

	// setup HTTP request
	migFile := "data/mig_request4remove.json"
	data, err := os.ReadFile(migFile)
	if err != nil {
		log.Printf("ERROR: unable to read %s error %v", migFile, err.Error())
		t.Fatal(err.Error())
	}
	reader := bytes.NewReader(data)

	// submit migration request
	rr, err := respRecorder("POST", "/dbs2go/submit", reader, web.MigrationSubmitHandler)
	if err != nil {
		t.Fatal(err)
	}

	// unmarshal received records
	var reports []dbs.MigrationReport
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &reports)
	if err != nil {
		t.Fatalf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Println("Received data", string(data))
	var rids []int64
	for _, rrr := range reports {
		req := rrr.MigrationRequest
		if req.MIGRATION_STATUS != 0 {
			t.Fatalf("invalid return status of migration request %+v", rrr)
		}
		rids = append(rids, req.MIGRATION_REQUEST_ID)
	}
	if len(rids) != 1 {
		t.Fatalf("wrong number of migration requests: found %d but expect only 1", len(rids))
	}

	// prepare cancel request
	request := make(map[string]int64)
	request["migration_rqst_id"] = rids[0]
	data, err = json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	reader = bytes.NewReader(data)

	// remove migration request
	_, err = respRecorder("POST", "/dbs2go/cancel", reader, web.MigrationCancelHandler)
	if err != nil {
		t.Fatal(err)
	}

	// fetch all available migration requests
	rr, err = respRecorder("GET", "dbs2go/status", reader, web.MigrationStatusHandler)
	if err != nil {
		t.Fatal(err)
	}
	var statusRecords []dbs.MigrationRequest
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &statusRecords)
	if err != nil {
		t.Fatalf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	log.Println("Received data", string(data))
	// we should expect one cancelled request
	var foundCancelledRequest bool
	var removeRecords []dbs.MigrationRemoveRequest
	for _, rrr := range statusRecords {
		mstatus := rrr.MIGRATION_STATUS
		if mstatus == 9 {
			foundCancelledRequest = true
		}
		// prepare migration request records to be used later in /remove API
		rec := dbs.MigrationRemoveRequest{
			MIGRATION_REQUEST_ID: rrr.MIGRATION_REQUEST_ID,
			CREATE_BY:            rrr.CREATE_BY,
		}
		removeRecords = append(removeRecords, rec)
	}
	if !foundCancelledRequest {
		t.Fatal("we did not found cancelled migration request")
	}

	// remove migration request
	for _, rec := range removeRecords {
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
		rr, err = respRecorder("POST", "/dbs2go/remove", reader, web.MigrationRemoveHandler)
		if err != nil {
			t.Fatal(err)
		}
		data = rr.Body.Bytes()
		log.Println("/remove output", string(data))
	}

	// check again status of migration, now we should see it is empty
	rr, err = respRecorder("GET", "dbs2go/status", reader, web.MigrationStatusHandler)
	if err != nil {
		t.Fatal(err)
	}
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &statusRecords)
	if err != nil {
		t.Fatalf("unable to unmarshal received data '%s', error %v", string(data), err)
	}
	if len(statusRecords) != 0 {
		t.Fatalf("wrong number of migration records, received %v, but expect 0", statusRecords)
	}
}
