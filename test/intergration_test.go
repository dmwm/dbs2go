package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"

	limiter "github.com/ulule/limiter/v3"
	stdlib "github.com/ulule/limiter/v3/drivers/middleware/stdlib"
	memory "github.com/ulule/limiter/v3/drivers/store/memory"
)

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
func runTestServer(t *testing.T, serverType string, lexiconFile string) *httptest.Server {
	dbfile := os.Getenv("DBS_DB_FILE")
	if dbfile == "" {
		t.Fatal("no DBS_DB_FILE env variable, please define")
	}
	web.Config.Base = "/dbs"
	web.Config.DBFile = dbfile
	web.Config.LexiconFile = lexiconFile
	web.Config.ServerCrt = ""
	web.Config.ServerKey = ""
	web.Config.ServerType = serverType
	web.Config.LogFile = "/tmp/dbs2go-test.log"
	web.Config.Verbose = 0

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
func verifyResponse(t *testing.T, received []dbs.Record, expected []Response, fields []string) {
	if len(received) != len(expected) {
		t.Fatalf("Expected length: %v, Received length: %v", len(expected), len(received))
	}
	fmt.Printf("Received: %s\nExpected: %s\n", received, expected)
	for _, f := range fields {
		for i, r := range received {
			if r[f] != expected[i][f] {
				if f == "creation_date" {
					if r[f] == nil {
						t.Fatalf("Field empty: %s", f)
					}
				} else {
					fmt.Printf("Received: %T, Expected: %T", r[f], expected[i][f])
					t.Fatalf("Incorrect %s: Expected: %v, Received: %v", f, expected[i][f], r[f])
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
/*
func runTestWorkflow(t *testing.T, tsR *httptest.Server, tsW *httptest.Server, endpoint string, hdlr func(http.ResponseWriter, *http.Request), reqBody map[string]string, fields []string, params url.Values, needParams bool) {
	// emap := remapRecord(t, dbrec)
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test GET with empty DB", func(t *testing.T) {
		d, _ := getData(t, tsR.URL, endpoint, params, needParams, http.StatusOK)
		if len(d) != 0 {
			t.Fatal("Data exists")
		}
	})

	t.Run("Test POST", func(t *testing.T) {
		records := injectDBRecord(t, body, "POST", tsW.URL, endpoint, params, hdlr, needParams, http.StatusOK)
		verifyResponse(t, records, reqBody, fields)
	})

	t.Run("Test GET after POST", func(t *testing.T) {
		d, _ := getData(t, tsR.URL, endpoint, params, needParams, http.StatusOK)
		verifyResponse(t, d, reqBody, fields)
	})

	t.Run("Test GET with parameters", func(t *testing.T) {
		getData(t, tsR.URL, endpoint, params, needParams, http.StatusOK)
	})

	t.Run("Test GET without parameters", func(t *testing.T) {
		getData(t, tsR.URL, endpoint, nil, needParams, http.StatusOK)
	})
}
*/

func runTestWorkflow(t *testing.T, c EndpointTestCase, tsR *httptest.Server, tsW *httptest.Server) {
	t.Run(c.description, func(t *testing.T) {
		for _, v := range c.testCases {
			t.Run(v.description, func(t *testing.T) {
				if v.method == "GET" {
					d, _ := getData(t, tsR.URL, v.endpoint, v.params, v.respCode)
					verifyResponse(t, d, v.resp, v.fields)
				} else if v.method == "POST" {
					injectDBRecord(t, v.record, tsW.URL, v.endpoint, v.params, v.handler, v.respCode)
				}
			})
		}
	})
}

// remap a dbs.DBRecord to a general dbs.Record
func remapRecord(t *testing.T, record dbs.DBRecord) dbs.Record {
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err.Error())
	}
	var emap dbs.Record
	err = json.Unmarshal(data, &emap)
	if err != nil {
		t.Fatal(err.Error())
	}
	return emap
}

// TestDBSIntegration Tests both DBSReader and DBSWriter Endpoints
func TestIntegation(t *testing.T) {
	db := initDB(false)
	defer db.Close()

	lexiconFileWriter := os.Getenv("DBS_WRITER_LEXICON_FILE")
	if lexiconFileWriter == "" {
		t.Fatal("no DBS_WRITER_LEXICON_FILE env variable, please define")
	}

	lexiconFileReader := os.Getenv("DBS_READER_LEXICON_FILE")
	if lexiconFileReader == "" {
		t.Fatal("no DBS_READER_LEXICON_FILE env variable, please define")
	}

	// start DBSWriter server
	tsW := runTestServer(t, "DBSWriter", lexiconFileWriter)
	defer tsW.Close()

	// start DBSReader server
	tsR := runTestServer(t, "DBSReader", lexiconFileReader)
	defer tsR.Close()

	/*
		t.Run("Test primarydataset", func(t *testing.T) {
			rec := map[string]string{
				"primary_ds_name": "unittest",
				"primary_ds_type": "test",
				"create_by":       "tester",
			}

			var fields = []string{}

			params := url.Values{}
			params.Add("primary_ds_name", "unittest")
			params.Add("primary_ds_type", "test")

			runTestWorkflow(t, tsR, tsW, "/dbs/primarydatasets", web.PrimaryDatasetsHandler, rec, fields, params, false)
		})
	*/

	/*
		t.Run("Test datatiers", func(t *testing.T) {
			rec := map[string]string{
				"data_tier_name": "GEN-SIM-RAW",
				"create_by":      "tester",
			}

			// fields that are created thru api handler
			var fields = []string{
				"data_tier_id",
				"creation_date",
				"data_tier_name",
				"create_by",
			}

			params := url.Values{}
			params.Add("data_tier_name", "GEN-SIM-RAW")

			runTestWorkflow(t, tsR, tsW, "/dbs/datatiers", web.DatatiersHandler, rec, fields, params, false)
		})
	*/

	/*
		t.Run("Test physicsgroups", func(t *testing.T) {
			rec := dbs.PhysicsGroups{
				PHYSICS_GROUP_NAME: "Tracker",
			}

			data, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}

			// fields that are created thru api handler
			fields := []string{"physics_group_name"}

			params := url.Values{}
			params.Add("physics_group_name", "Tracker")

			runTestWorkflow(t, tsR, tsW, "/dbs/physicsgroups", web.PhysicsGroupsHandler, data, fields, params, false)
		})

		t.Run("Test datasetaccesstypes", func(t *testing.T) {
			rec := dbs.DatasetAccessTypes{
				DATASET_ACCESS_TYPE: "PRODUCTION",
			}

			data, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}

			fields := []string{"dataset_access_type"}

			params := url.Values{}
			params.Add("dataset_access_type", "PRODUCTION")

			runTestWorkflow(t, tsR, tsW, "/dbs/datasetaccesstypes", web.DatasetAccessTypesHandler, data, fields, params, false)
		})
	*/
}

func TestIntegration2(t *testing.T) {
	db := initDB(false)
	defer db.Close()

	lexiconFileWriter := os.Getenv("DBS_WRITER_LEXICON_FILE")
	if lexiconFileWriter == "" {
		t.Fatal("no DBS_WRITER_LEXICON_FILE env variable, please define")
	}

	lexiconFileReader := os.Getenv("DBS_READER_LEXICON_FILE")
	if lexiconFileReader == "" {
		t.Fatal("no DBS_READER_LEXICON_FILE env variable, please define")
	}

	// start DBSWriter server
	tsW := runTestServer(t, "DBSWriter", lexiconFileWriter)
	defer tsW.Close()

	// start DBSReader server
	tsR := runTestServer(t, "DBSReader", lexiconFileReader)
	defer tsR.Close()

	for _, v := range IntegrationTestCases {
		runTestWorkflow(t, v, tsR, tsW)
	}

}
