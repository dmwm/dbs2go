package main

import (
	"bytes"
	"encoding/json"
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
func configTestServer(t *testing.T, serverType string, lexiconFile string) {
	web.Config.Base = "/dbs"
	web.Config.DBFile = "../dbfile"
	web.Config.LexiconFile = lexiconFile
	web.Config.ServerCrt = ""
	web.Config.ServerKey = ""
	web.Config.ServerType = serverType
	web.Config.LogFile = "/tmp/dbs2go-test.log"
	web.Config.Verbose = 0

	initTestLimiter(t, "100-S")
}

// injects dbs records
func injectDBRecord(t *testing.T, rec dbs.DBRecord, method string, url string, endpoint string, handler func(http.ResponseWriter, *http.Request)) []dbs.Record {
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err.Error())
	}
	reader := bytes.NewReader(data)
	req := newreq(t, method, url, endpoint, reader, nil)
	req.Header.Add("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatal(err)
	}

	rr, err := respRecorder("GET", endpoint, nil, handler)
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
func getData(t *testing.T, url string, endpoint string, params url.Values) ([]dbs.Record, int) {
	r, err := http.DefaultClient.Do(newreq(t, "GET", url, endpoint, nil, params))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("Bad status code: %v", r.StatusCode)
	}

	var d []dbs.Record
	err = json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		t.Fatalf("Failed to decode body, %v", err)
	}

	return d, r.StatusCode
}

// creates an http request for testing
func newreq(t *testing.T, method string, hostname string, endpoint string, body io.Reader, params url.Values) *http.Request {
	url2, err := url.Parse(hostname)
	if err != nil {
		t.Fatal(err)
	}
	url2.Path = endpoint
	url2.RawQuery = params.Encode()

	r, err := http.NewRequest(method, url2.String(), body)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestDBSIntegration Tests both DBSReader and DBSWriter Endpoints
func TestDBSIntegration(t *testing.T) {
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

	configTestServer(t, "DBSWriter", lexiconFileWriter)
	// start DBSWriter server
	tsW := httptest.NewServer(web.Handlers())
	defer tsW.Close()

	configTestServer(t, "DBSReader", lexiconFileReader)
	// start DBSReader server
	tsR := httptest.NewServer(web.Handlers())
	defer tsR.Close()

	t.Run("Test datatiers", func(t *testing.T) {
		// verify datatier received vs data
		verifyResponse := func(expected dbs.DataTiers, received []dbs.Record) {
			if received[0]["data_tier_name"] != expected.DATA_TIER_NAME {
				t.Fatalf("Incorrect data_tier_name: Expected %v, Received %v", expected.DATA_TIER_NAME, received[0]["data_tier_name"])
			}
			if received[0]["create_by"] != expected.CREATE_BY {
				t.Fatalf("Incorrect create_by: Expected %v, Received %v", expected.CREATE_BY, received[0]["create_by"])
			}
			if received[0]["data_tier_id"] == nil {
				t.Fatalf("No ID assigned")
			}
		}

		dt := dbs.DataTiers{
			DATA_TIER_NAME: "GEN-SIM-RAW",
			CREATE_BY:      "tester",
		}

		t.Run("Test empty GET", func(t *testing.T) {
			d, _ := getData(t, tsR.URL, "/dbs/datatiers", nil)
			if len(d) != 0 {
				t.Fatal("Data exists")
			}
		})

		t.Run("Test POST", func(t *testing.T) {
			records := injectDBRecord(t, &dt, "POST", tsW.URL, "/dbs/datatiers", web.DatatiersHandler)
			verifyResponse(dt, records)
		})

		t.Run("Test GET after POST", func(t *testing.T) {
			d, _ := getData(t, tsR.URL, "/dbs/datatiers", nil)
			verifyResponse(dt, d)
		})

		t.Run("Test GET with parameters", func(t *testing.T) {
			params := url.Values{}
			params.Add("data_tier_name", "GEN-SIM-RAW")
			getData(t, tsR.URL, "/dbs/datatiers", params)
		})
	})

	t.Run("Test physicsgroups", func(t *testing.T) {
		// verify physicsgroups received vs expected data
		verifyResponse := func(expected dbs.PhysicsGroups, received []dbs.Record) {
			if received[0]["physics_group_name"] != expected.PHYSICS_GROUP_NAME {
				t.Fatalf("Incorrect physics_group_name: Expected %v, Received %v", expected.PHYSICS_GROUP_NAME, received[0]["physics_group_name"])
			}
			if received[0]["physics_group_id"] != nil {
				t.Fatalf("No ID assigned")
			}
		}

		pg := dbs.PhysicsGroups{
			PHYSICS_GROUP_NAME: "Tracker",
		}

		t.Run("Test empty GET", func(t *testing.T) {
			d, _ := getData(t, tsR.URL, "/dbs/physicsgroups", nil)
			if len(d) != 0 {
				t.Fatal("Data exists")
			}
		})

		t.Run("Test POST", func(t *testing.T) {
			records := injectDBRecord(t, &pg, "POST", tsW.URL, "/dbs/physicsgroups", web.PhysicsGroupsHandler)
			verifyResponse(pg, records)
		})

		t.Run("Test GET after POST", func(t *testing.T) {
			d, _ := getData(t, tsR.URL, "/dbs/physicsgroups", nil)
			verifyResponse(pg, d)
		})

		t.Run("Test GET with parameters", func(t *testing.T) {
			params := url.Values{}
			params.Add("physics_group_name", "Tracker")
			getData(t, tsR.URL, "/dbs/physicsgroups", params)
		})
	})
}
