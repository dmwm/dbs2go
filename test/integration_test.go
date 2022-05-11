package main

// Integration Tests
// This file contains the main function, TestIntegration, for running DBS integration tests.
// The DBS integration tests are table-driven
// The tests workflow is as follows:
//   1. Load data into test tables
//   2. Iterate through tables, providing the data to runTestWorkflow
//   3. Depending on configuration in table, start a fully running DBSReader or DBSWriter server
//   4. Execute the HTTP request and validate results
// Middlewares can also be tested, such as the validation middleware
// The tables for each of the endpoints is defined in test/int_*.go files
// Default data for the tables are loaded in test/data/integration/integration_data.json.
// Data is loaded into the tables in test/data/integration_cases.go

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	_ "github.com/mattn/go-sqlite3"
)

// run test workflow for a single endpoint
func runTestWorkflow(t *testing.T, c EndpointTestCase) {

	var server *httptest.Server

	t.Run(c.description, func(t *testing.T) {
		for _, v := range c.testCases {
			t.Run(v.description, func(t *testing.T) {

				// set the default handler
				/*
					handler := c.defaultHandler
					if v.handler != nil {
						handler = v.handler
					}
				*/

				// set the endpoint
				endpoint := c.defaultEndpoint
				if v.endpoint != "" {
					endpoint = v.endpoint
				}

				// run a test server for a single test case
				server = dbsServer(t, "dbs", "DBS_DB_FILE", v.serverType)
				defer server.Close()

				// create request body
				data, err := json.Marshal(v.input)
				if err != nil {
					t.Fatal(err.Error())
				}
				reader := bytes.NewReader(data)

				// Set headers
				headers := http.Header{
					"Accept":          []string{"application/json"},
					"Content-Type":    []string{"application/json"},
					"Accept-Encoding": []string{"identity"},
				}
				req := newreq(t, v.method, server.URL, endpoint, reader, v.params, headers)

				// execute request
				r, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err.Error())
				}
				defer r.Body.Close()

				// ensure returned status code is same as expected status code
				if r.StatusCode != v.respCode {
					t.Fatalf("Different HTTP Status: Expected %v, Received %v", v.respCode, r.StatusCode)
				}

				var d []dbs.Record
				// decode and verify the GET request
				if v.method == "GET" {
					err = json.NewDecoder(r.Body).Decode(&d)
					if err != nil {
						t.Fatalf("Failed to decode body, %v", err)
					}

					verifyResponse(t, d, v.output)
				}
				/*
					if v.method == "POST" {
						rURL := parseURL(t, server.URL, endpoint, v.params)
						rr, err := respRecorder("GET", rURL.RequestURI(), nil, handler)
						if err != nil {
							t.Error(err)
						}
						data = rr.Body.Bytes()
						err = json.Unmarshal(data, &d)
						if err != nil {
							t.Fatal(err)
						}
					}
				*/
			})
		}
	})
}

// TestIntegration Tests both DBSReader and DBSWriter Endpoints
func TestIntegration(t *testing.T) {
	db := initDB(false)
	defer db.Close()

	testCaseFile := os.Getenv("INTEGRATION_DATA_FILE")
	if testCaseFile == "" {
		log.Fatal("INTEGRATION_DATA_FILE not defined")
	}
	bulkblocksFile := os.Getenv("BULKBLOCKS_DATA_FILE")
	if bulkblocksFile == "" {
		log.Fatal("BULKBLOCKS_DATA_FILE not defined")
	}

	testCases := LoadTestCases(t, testCaseFile, bulkblocksFile)

	for _, v := range testCases {
		runTestWorkflow(t, v)
	}
}
