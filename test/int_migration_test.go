package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dmwm/dbs2go/dbs"
)

// TestLocalMigrationRequests test local DBS Migration requests
func TestMigrationRequests2(t *testing.T) {
	var GrandparentBulkBlocksData dbs.BulkBlocks
	var ParentBulkBlocksData dbs.BulkBlocks
	var BulkBlocksData dbs.BulkBlocks

	bulkblocksPath := os.Getenv("MIGRATION_REQUESTS_PATH")
	if bulkblocksPath == "" {
		log.Fatal("MIGRATION_REQUESTS_PATH not defined")
	}

	// load grandparent data
	err := readJsonFile(t, fmt.Sprintf("%s/genericttbar_grandparent_block.json", bulkblocksPath), &GrandparentBulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	// load parent data
	err = readJsonFile(t, fmt.Sprintf("%s/genericttbar_parent_block.json", bulkblocksPath), &ParentBulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	// load block data
	err = readJsonFile(t, fmt.Sprintf("%s/genericttbar_block.json", bulkblocksPath), &BulkBlocksData)
	if err != nil {
		t.Fatal(err.Error())
	}

	cmd := exec.Command("../bin/start_test_migration")
	err = cmd.Start()
	pid := cmd.Process.Pid
	t.Log(pid)
	if err != nil {
		t.Fatal(err.Error())
	}
	cmd.Wait()

	t.Cleanup(func() {
		exec.Command("pkill", "dbs2go").Output()
	})

	t.Run("Insert initial data", func(t *testing.T) {

		t.Run("Grandparent blocks", func(t *testing.T) {
			data, err := json.Marshal(GrandparentBulkBlocksData)
			if err != nil {
				t.Fatal(err.Error())
			}
			reader := bytes.NewReader(data)

			resp, err := http.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
			if err != nil {
				t.Error(err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Bad HTTP Response: %d Expected, %d Received\n", http.StatusOK, resp.StatusCode)
			}
		})

		t.Run("Parent blocks", func(t *testing.T) {
			data, err := json.Marshal(ParentBulkBlocksData)
			if err != nil {
				t.Fatal(err.Error())
			}
			reader := bytes.NewReader(data)

			resp, err := http.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
			if err != nil {
				t.Error(err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Bad HTTP Response: %d Expected, %d Received\n", http.StatusOK, resp.StatusCode)
			}
		})

		t.Run("Blocks", func(t *testing.T) {
			data, err := json.Marshal(BulkBlocksData)
			if err != nil {
				t.Fatal(err.Error())
			}
			reader := bytes.NewReader(data)

			resp, err := http.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
			if err != nil {
				t.Error(err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Bad HTTP Response: %d Expected, %d Received\n", http.StatusOK, resp.StatusCode)
			}
		})
	})

	// delay for debugging
	time.Sleep(1 * time.Minute)

	t.Run("Test migration", func(t *testing.T) {
		var d []dbs.MigrationReport
		t.Run("Submit Migration Request", func(t *testing.T) {

			// ds := GrandparentBulkBlocksData.Dataset.Dataset
			// ds := ParentBulkBlocksData.Dataset.Dataset
			blk := BulkBlocksData.Block.BlockName

			migReq := MigrationRequest{
				MigrationURL:   "http://localhost:8989/dbs-one-reader",
				MigrationInput: blk,
			}

			body, err := json.Marshal(migReq)
			if err != nil {
				t.Fatalf("Failed to marshal json")
			}
			resp, err := http.Post("http://localhost:8993/dbs-migrate/submit", "application/json", bytes.NewBuffer(body))
			if err != nil {
				t.Fatalf("HTTP Request Error: %v\n", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Logf("Response %v", resp)
				t.Fatalf("Not Status OK: %v", resp.StatusCode)
			}

			err = json.NewDecoder(resp.Body).Decode(&d)
			if err != nil {
				t.Fatalf("Failed to decode body, %v", err)
			}

			fmt.Printf("%+v\n", d)
		})

		for _, migreq := range d {
			// var failedMigrations int

			mid := migreq.MigrationRequest.MIGRATION_REQUEST_ID
			desc := fmt.Sprintf("Process migration request, id = %d", mid)
			t.Run(desc, func(t *testing.T) {
				req := processMigrationRequest{
					MigrationRequestId: mid,
				}

				body, err := json.Marshal(req)
				if err != nil {
					t.Fatalf("Failed to marshal json")
				}
				uri := fmt.Sprintf("http://localhost:8993/dbs-migrate/process?migration_request_id=%d", mid)
				resp, err := http.Post(uri, "application/json", bytes.NewBuffer(body))
				if err != nil {
					t.Fatalf("HTTP Request Error: %v\n", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					t.Logf("Response %v", resp)
					t.Fatalf("Not Status OK: %v", resp.StatusCode)
				}

				fmt.Printf("%+v\n", resp.Body)

			})
		}

		// get current time
		now := time.Now()
		migTimeout := 3 * time.Minute

		t.Run("Check Migration Request status", func(t *testing.T) {
			var requestStatus [10]int
			requestStatus[0] = len(d)
			for requestStatus[0] != 0 || requestStatus[1] != 0 || requestStatus[5] != 0 || time.Since(now) > migTimeout {
				// sleep in order to prevent sqlite database locking
				time.Sleep(10 * time.Second)

				var migrationStatus []MigrationStatus

				requestStatus = [10]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

				resp, err := http.Get("http://localhost:8993/dbs-migrate/status")
				if err != nil {
					t.Fatalf("HTTP Request Error: %v\n", err)
				}
				defer resp.Body.Close()

				err = json.NewDecoder(resp.Body).Decode(&migrationStatus)
				if err != nil {
					t.Fatalf("Failed to decode body, %v", err)
				}

				for _, m := range migrationStatus {
					requestStatus[m.MigrationStatus] += 1
				}
				fmt.Printf("Number of Requests: %d, Successful: %d, Statuses: %v\n", len(d), requestStatus[2], requestStatus)
			}

			if requestStatus[9] > 0 {
				t.Fatalf("%d requests failed\n", requestStatus[9])
			}

			if time.Since(now) > migTimeout {
				t.Fatalf("Migration request timeout")
			}
		})
	})
}
