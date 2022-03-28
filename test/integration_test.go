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
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/utils"
	"github.com/dmwm/dbs2go/web"
	_ "github.com/mattn/go-sqlite3"

	diff "github.com/r3labs/diff/v2"
	limiter "github.com/ulule/limiter/v3"
	stdlib "github.com/ulule/limiter/v3/drivers/middleware/stdlib"
	memory "github.com/ulule/limiter/v3/drivers/store/memory"
)

// initializes the limiter middleware
func initTestLimiter(t *testing.T, period string) {
	rate, err := limiter.NewRateFromFormatted(period)
	if err != nil {
		log.Fatalf("Limiter Error")
	}
	store := memory.NewStore()
	instance := limiter.New(store, rate)
	web.LimiterMiddleware = stdlib.NewMiddleware(instance)
}

// configures the test server with basic parameters
func runTestServer(t *testing.T, serverType string) *httptest.Server {
	dbfile := os.Getenv("DBS_DB_FILE")
	if dbfile == "" {
		log.Fatal("no DBS_DB_FILE env variable, please define")
	}

	var lexiconFile string

	if serverType == "DBSWriter" {
		lexiconFile = os.Getenv("DBS_WRITER_LEXICON_FILE")
		if lexiconFile == "" {
			log.Fatal("no DBS_WRITER_LEXICON_FILE env variable, please define")
		}
	} else if serverType == "DBSReader" {
		lexiconFile = os.Getenv("DBS_READER_LEXICON_FILE")
		if lexiconFile == "" {
			log.Fatal("no DBS_READER_LEXICON_FILE env variable, please define")
		}
	}

	web.Config.Base = "/dbs"
	web.Config.DBFile = dbfile
	web.Config.LexiconFile = lexiconFile
	web.Config.ServerCrt = ""
	web.Config.ServerKey = ""
	web.Config.ServerType = serverType
	web.Config.LogFile = "/tmp/dbs2go-test.log"
	web.Config.Verbose = 0
	utils.VERBOSE = 0
	utils.BASE = "/dbs"
	lexPatterns, err := dbs.LoadPatterns(lexiconFile)
	if err != nil {
		t.Fatal(err)
	}
	dbs.LexiconPatterns = lexPatterns

	initTestLimiter(t, "100-S")

	ts := httptest.NewServer(web.Handlers())

	return ts
}

// creates a URL given a hostname, endpoint, and parameters
func parseURL(t *testing.T, hostname string, endpoint string, params url.Values) *url.URL {
	url2, err := url.Parse(hostname)
	if err != nil {
		t.Fatal(err)
	}
	url2.Path = endpoint
	url2.RawQuery = params.Encode()

	return url2
}

// creates an http request for testing
func newreq(t *testing.T, method string, hostname string, endpoint string, body io.Reader, params url.Values, headers http.Header) *http.Request {
	reqURL := parseURL(t, hostname, endpoint, params)

	r, err := http.NewRequest(method, reqURL.String(), body)
	if err != nil {
		t.Fatal(err)
	}

	r.Header = headers

	return r
}

// convert []Response to []dbs.Record
func responseToRecord(t *testing.T, rec []Response) []dbs.Record {
	d, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}

	var e []dbs.Record
	err = json.Unmarshal(d, &e)
	if err != nil {
		t.Fatal(err)
	}

	return e
}

// compares received response to expected
func verifyResponse(t *testing.T, received []dbs.Record, expected []Response) {
	expect := expected
	if expected == nil {
		expect = []Response{}
	}
	log.Printf("\nReceived: %v\nExpected: %+v\n", received, expect)
	if len(received) != len(expect) {
		t.Fatalf("Expected length: %v, Received length: %v", len(expect), len(received))
	}

	e := responseToRecord(t, expect)

	// fields not in initial POST request
	generatedFields := []string{
		"creation_date", // created upon POST
		"start_date",
		"end_date",
		"http", // client http information on errors
	}

	for i, r := range received {
		log.Printf("\nReceived: %#v\nExpected: %#v\n", r, e[i])
		// see difference between expected and received structs
		c, err := diff.Diff(e[i], r)
		if err != nil {
			t.Fatal(err)
		}
		// Check if the changes are from generated values
		for _, a := range c {
			field := a.Path[0]
			if utils.InList(field, generatedFields) {
				// check if a value was given to the field
				if a.To == nil {
					t.Fatalf("Field empty: %s", field)
				}
			} else {
				t.Fatalf("Incorrect %s, received %s, expected %s", field, a.To, a.From)
			}
		}
	}
}

// injects dbs records
func injectDBRecord(t *testing.T, rec RequestBody, hostname string, endpoint string, params url.Values, handler func(http.ResponseWriter, *http.Request), httpCode int) []dbs.Record {
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err.Error())
	}
	reader := bytes.NewReader(data)
	headers := http.Header{
		"Accept":       []string{"application/json"},
		"Content-Type": []string{"application/json"},
	}
	r, err := http.DefaultClient.Do(newreq(t, "POST", hostname, endpoint, reader, nil, headers))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()

	if r.StatusCode != httpCode {
		t.Fatalf("Different HTTP Status: Expected %v, Received %v", httpCode, r.StatusCode)
	}

	rURL := parseURL(t, hostname, endpoint, params)

	rr, err := respRecorder("GET", rURL.RequestURI(), nil, handler)
	if err != nil {
		t.Error(err)
	}

	var records []dbs.Record
	data = rr.Body.Bytes()
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Fatal(err)
	}

	return records
}

// fetches data from url and endpoint
func getData(t *testing.T, url string, endpoint string, params url.Values, httpCode int) ([]dbs.Record, int) {
	r, err := http.DefaultClient.Do(newreq(t, "GET", url, endpoint, nil, params, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()

	if r.StatusCode != httpCode {
		t.Fatalf("Bad status code: %v", r.StatusCode)
	}

	var d []dbs.Record
	err = json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}

	return d, r.StatusCode
}

// run test workflow for a single endpoint
func runTestWorkflow(t *testing.T, c EndpointTestCase) {

	var server *httptest.Server

	t.Run(c.description, func(t *testing.T) {
		for _, v := range c.testCases {
			t.Run(v.description, func(t *testing.T) {
				var handler func(http.ResponseWriter, *http.Request)

				// set handler
				handler = c.defaultHandler
				if v.handler != nil {
					handler = v.handler
				}

				// set the endpoint
				endpoint := c.defaultEndpoint
				if v.endpoint != "" {
					endpoint = v.endpoint
				}
				// run a test server for a single test case
				server = runTestServer(t, v.serverType)
				defer server.Close()
				if v.method == "GET" {
					d, _ := getData(t, server.URL, endpoint, v.params, v.respCode)
					verifyResponse(t, d, v.output)
				} else if v.method == "POST" {
					injectDBRecord(t, v.input, server.URL, endpoint, v.params, handler, v.respCode)
				}
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

	testCases := LoadTestCases(t, testCaseFile)

	for _, v := range testCases {
		runTestWorkflow(t, v)
	}
}
