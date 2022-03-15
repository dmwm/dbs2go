package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
	"github.com/vkuznet/dbs2go/web"

	limiter "github.com/ulule/limiter/v3"
	stdlib "github.com/ulule/limiter/v3/drivers/middleware/stdlib"
	memory "github.com/ulule/limiter/v3/drivers/store/memory"
)

// reads a json file
func readJsonFile(t *testing.T, filename string) map[string]interface{} {
	var data []byte
	var err error
	var testData map[string]interface{}
	data, err = os.ReadFile(filename)
	if err != nil {
		log.Printf("ERROR: unable to read %s error %v", filename, err.Error())
		t.Fatal(err.Error())
	}
	err = json.Unmarshal(data, &testData)
	if err != nil {
		log.Println("unable to unmarshal received data")
		t.Fatal(err.Error())
	}
	return testData
}

// initializes the limiter middleware
func initTestLimiter(t *testing.T, period string) {
	rate, err := limiter.NewRateFromFormatted(period)
	if err != nil {
		t.Fatalf("Limiter Error")
	}
	store := memory.NewStore()
	instance := limiter.New(store, rate)
	web.LimiterMiddleware = stdlib.NewMiddleware(instance)
}

// configures the test server with basic parameters
func runTestServer(t *testing.T, serverType string) *httptest.Server {
	dbfile := os.Getenv("DBS_DB_FILE")
	if dbfile == "" {
		t.Fatal("no DBS_DB_FILE env variable, please define")
	}

	var lexiconFile string

	if serverType == "DBSWriter" {
		lexiconFile = os.Getenv("DBS_WRITER_LEXICON_FILE")
		if lexiconFile == "" {
			t.Fatal("no DBS_WRITER_LEXICON_FILE env variable, please define")
		}
	} else if serverType == "DBSReader" {
		lexiconFile = os.Getenv("DBS_READER_LEXICON_FILE")
		if lexiconFile == "" {
			t.Fatal("no DBS_READER_LEXICON_FILE env variable, please define")
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
func newreq(t *testing.T, method string, hostname string, endpoint string, body io.Reader, params url.Values) *http.Request {
	reqURL := parseURL(t, hostname, endpoint, params)

	r, err := http.NewRequest(method, reqURL.String(), body)
	if err != nil {
		t.Fatal(err)
	}

	return r
}

// compares received response to expected
func verifyResponse(t *testing.T, received []dbs.Record, expected []Response) { //, fields []string) {
	expect := expected
	if expected == nil {
		expect = []Response{}
	}
	fmt.Printf("Received: %s\nExpected: %s\n", received, expect)
	if len(received) != len(expect) {
		t.Fatalf("Expected length: %v, Received length: %v", len(expect), len(received))
	}
	fields := []string{}
	if len(expect) > 0 {
		fields = utils.MapKeys(expect[0])
	}
	for _, f := range fields {
		for i, r := range received {
			if f == "error" {
				rCode := r[f].(map[string]interface{})["code"]
				eCode := expect[i]["error"]
				if rCode != eCode {
					t.Fatalf("Expected error code: %v, Received: %v", rCode, eCode)
				}
			} else {
				if r[f] != expect[i][f] {
					if strings.Contains(f, "date") {
						if r[f] == nil {
							t.Fatalf("Field empty: %s", f)
						}
					} else {
						t.Fatalf("Incorrect %s: Expected: %v, Received: %v", f, expect[i][f], r[f])
					}
				}
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
	req := newreq(t, "POST", hostname, endpoint, reader, nil)
	req.Header.Add("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	r, err := http.DefaultClient.Do(req)
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
	r, err := http.DefaultClient.Do(newreq(t, "GET", url, endpoint, nil, params))
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
func runTestWorkflow(t *testing.T, c EndpointTestCase) { //, tsR *httptest.Server, tsW *httptest.Server) {

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

	testData := readJsonFile(t, "./data/integrationdata.json")
	fmt.Println(testData)

	testCases := LoadTestCases(t)

	for _, v := range testCases {
		runTestWorkflow(t, v)
	}
}
