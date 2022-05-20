package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
	validator "github.com/go-playground/validator/v10"
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

// helper function to initialize DB for tests
func initDB(dryRun bool) *sql.DB {
	log.SetFlags(0)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// current directory is a <pwd>/test
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("unable to get current working dir")
	}
	utils.STATICDIR = fmt.Sprintf("%s/../static", dir)
	utils.VERBOSE = 1
	dbtype := "sqlite3"
	dburi := "/tmp/dbs-test.db"
	dbowner := "sqlite"

	db, err := sql.Open(dbtype, dburi)
	if err != nil {
		log.Fatal("unable to open db file", err)
	}
	dbs.DB = db
	dbs.MigrationDB = db
	dbs.DBTYPE = dbtype
	dbsql := dbs.LoadSQL(dbowner)
	dbs.DBSQL = dbsql
	dbs.DBOWNER = dbowner
	if dryRun {
		dbs.DRYRUN = true
	}
	// init validator
	dbs.RecordValidator = validator.New()
	dbs.FileLumiChunkSize = 1000

	// init parameters file
	if dbs.ApiParametersFile == "" {
		apiParametersFile := os.Getenv("DBS_API_PARAMETERS_FILE")
		if apiParametersFile == "" {
			log.Fatal("no DBS_API_PARAMETERS_FILE env variable, please define")
		}
		dbs.ApiParametersFile = apiParametersFile
	}
	return db
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

// helper funtion to return DBS server with basic parameters
func dbsServer(t *testing.T, base, dbFile, serverType string, concurrent bool) *httptest.Server {
	dbfile := os.Getenv(dbFile)
	if dbfile == "" {
		log.Fatalf("no %s env variable, please define", dbFile)
	}

	var lexiconFile string

	if serverType == "DBSReader" {
		lexiconFile = os.Getenv("DBS_READER_LEXICON_FILE")
		if lexiconFile == "" {
			log.Fatal("no DBS_READER_LEXICON_FILE env variable, please define")
		}
	} else {
		lexiconFile = os.Getenv("DBS_WRITER_LEXICON_FILE")
		if lexiconFile == "" {
			log.Fatal("no DBS_WRITER_LEXICON_FILE env variable, please define")
		}
	}

	apiParametersFile := os.Getenv("DBS_API_PARAMETERS_FILE")
	if apiParametersFile == "" {
		log.Fatal("no DBS_API_PARAMETERS_FILE env variable, please define")
	}

	web.Config.Base = base
	web.Config.DBFile = dbfile
	web.Config.LexiconFile = lexiconFile
	web.Config.ApiParametersFile = apiParametersFile
	web.Config.ServerCrt = ""
	web.Config.ServerKey = ""
	web.Config.ServerType = serverType
	web.Config.LogFile = fmt.Sprintf("/tmp/dbs2go-%s.log", base)
	web.Config.Verbose = 0
	web.Config.ConcurrentBulkBlocks = concurrent
	utils.VERBOSE = 0
	utils.BASE = base
	lexPatterns, err := dbs.LoadPatterns(lexiconFile)
	if err != nil {
		t.Fatal(err)
	}
	dbs.LexiconPatterns = lexPatterns

	initTestLimiter(t, "100-S")

	ts := httptest.NewServer(web.Handlers())

	return ts
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

// create server error response
func createServerErrorResponse(hrec web.HTTPError, dbsError error) web.ServerError {
	return web.ServerError{
		HTTPError: hrec,
		DBSError:  dbsError,
		Exception: http.StatusBadRequest,
		Type:      "HTTPError",
		Message:   dbsError.Error(),
	}
}

// create HTTPError
func createHTTPError(method string, path string) web.HTTPError {
	return web.HTTPError{
		Method:    method,
		Timestamp: "",
		HTTPCode:  http.StatusBadRequest,
		Path:      path,
		UserAgent: "Go-http-client/1.1",
	}
}

// compares received response to expected
func verifyResponse(t *testing.T, received []dbs.Record, expected []Response) {
	expect := expected
	if expected == nil {
		expect = []Response{}
	}
	if testing.Verbose() {
		log.Printf("\nReceived: %v\nExpected: %+v\n", received, expect)
	}
	if len(received) != len(expect) {
		t.Fatalf("Expected length: %v, Received length: %v", len(expect), len(received))
	}

	e := responseToRecord(t, expect)

	// fields not in initial POST request
	generatedFields := []string{
		"creation_date",          // created upon POST
		"last_modification_date", // created upon POST
		"start_date",
		"end_date",
		"http", // client http information on errors
	}

	for i, r := range received {
		if testing.Verbose() {
			log.Printf("\nReceived: %#v\nExpected: %#v\n", r, e[i])
		}
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
					t.Fatalf("Field empty: %v", field)
				}
			} else {
				if a.To == nil && a.From == nil { // check if both values are nil
					t.Logf("Both values for field %v are nil", field)
				} else {
					t.Fatalf("Incorrect %v:\nreceived %v (%T),\nexpected %v (%T)", field, a.To, a.To, a.From, a.From)
				}
			}
		}
	}
}
