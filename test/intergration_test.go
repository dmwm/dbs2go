package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"

	limiter "github.com/ulule/limiter/v3"
	stdlib "github.com/ulule/limiter/v3/drivers/middleware/stdlib"
	memory "github.com/ulule/limiter/v3/drivers/store/memory"
)

func newreq(t *testing.T, method string, url string, body io.Reader) *http.Request {
	r, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func initTestLimiter(t *testing.T, period string) {
	rate, err := limiter.NewRateFromFormatted(period)
	if err != nil {
		t.Fatalf("Limiter Error")
	}
	store := memory.NewStore()
	instance := limiter.New(store, rate)
	web.LimiterMiddleware = stdlib.NewMiddleware(instance)
}

func configTestServer(t *testing.T, serverType string, lexiconFile string) {
	web.Config.Base = "/dbs"
	web.Config.DBFile = "../dbfile"
	web.Config.LexiconFile = lexiconFile
	web.Config.ServerCrt = ""
	web.Config.ServerKey = ""
	web.Config.ServerType = "DBSWriter"
	web.Config.LogFile = serverType
	web.Config.Verbose = 0

	initTestLimiter(t, "100-S")
}

func TestDBSWriterIntegration(t *testing.T) {
	db := initDB(false)
	defer db.Close()

	configTestServer(t, "DBSWriter", "../static/lexicon_writer.json")

	ts := httptest.NewServer(web.Handlers())
	defer ts.Close()

	t.Run("Test POST datatiers", func(t *testing.T) {
		var err error

		dt := dbs.DataTiers{
			DATA_TIER_NAME: "GEN-SIM-RAW",
			CREATE_BY:      "tester",
		}

		data, err := json.Marshal(dt)
		if err != nil {
			t.Fatal(err.Error())
		}
		reader := bytes.NewReader(data)
		req := newreq(t, "POST", ts.URL+"/dbs/datatiers", reader)
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

		rr, err := respRecorder("GET", "/dbs/datatiers", nil, web.DatatiersHandler)
		if err != nil {
			t.Error(err)
		}

		var records []dbs.Record
		data = rr.Body.Bytes()
		err = json.Unmarshal(data, &records)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(records[0]["create_by"])
		if records[0]["data_tier_name"] != "GEN-SIM-RAW" {
			t.Fatalf("Incorrect data_tier_name: Expected %v, Received %v", "GEN-SIM-RAW", records[0]["data_tier_name"])
		}
		if records[0]["create_by"] != "tester" {
			t.Fatalf("Incorrect create_by: Expected %v, Received %v", "tester", records[0]["create_by"])
		}
	})
}

// TestDBSReaderServer Endpoints
func TestDBSReaderIntegration(t *testing.T) {
	db := initDB(false)
	defer db.Close()

	configTestServer(t, "DBSReader", "../static/lexicon_reader.json")

	ts := httptest.NewServer(web.Handlers())
	defer ts.Close()

	t.Run("Test GET /datatiers", func(t *testing.T) {
		r, err := http.DefaultClient.Do(newreq(t, "GET", ts.URL+"/dbs/datatiers", nil))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			t.Fatalf("Bad status code: %v", r.Status)
		}

		var d []dbs.Record
		err = json.NewDecoder(r.Body).Decode(&d)
		if err != nil {
			t.Fatalf("Failed to decode body, %v", err)
		}

		if d[0]["data_tier_name"] != "GEN-SIM-RAW" {
			t.Fatalf("Incorrect data_tier_name: Expected %v, Received %v", "GEN-SIM-RAW", d[0]["data_tier_name"])
		}
		if d[0]["create_by"] != "tester" {
			t.Fatalf("Incorrect create_by: Expected %v, Received %v", "tester", d[0]["create_by"])
		}
	})
}
