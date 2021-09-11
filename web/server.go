package web

// DBS web server
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//
//
// Some links:  http://www.alexedwards.net/blog/golang-response-snippets
//              http://blog.golang.org/json-and-go
// Go patterns: http://www.golangpatterns.info/home
// Templates:   http://gohugo.io/templates/go-templates/
//              http://golang.org/pkg/html/template/
// Go examples: https://gobyexample.com/
// for Go database API: http://go-database-sql.org/overview.html
// Oracle drivers:
//   _ "gopkg.in/rana/ora.v4"
//   _ "github.com/mattn/go-oci8"
// MySQL driver:
//   _ "github.com/go-sql-driver/mysql"
// SQLite driver:
//  _ "github.com/mattn/go-sqlite3"
//
// Get profile's output
// visit http://localhost:<port>/debug/pprof
// or generate png plots
// go tool pprof -png http://localhost:<port>/debug/pprof/heap > /tmp/heap.png
// go tool pprof -png http://localhost:<port>/debug/pprof/profile > /tmp/profile.png

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	//     _ "github.com/go-sql-driver/mysql"
	_ "net/http/pprof"

	"github.com/dmwm/cmsauth"
	validator "github.com/go-playground/validator/v10"
	"github.com/gorilla/csrf"
	"github.com/gorilla/mux"
	graphql "github.com/graph-gophers/graphql-go"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/vkuznet/auth-proxy-server/logging"
	"github.com/vkuznet/dbs2go/dbs"
	dbsGraphQL "github.com/vkuznet/dbs2go/graphql"
	"github.com/vkuznet/dbs2go/utils"

	// imports for supported DB drivers
	// go-oci8 oracle driver
	_ "github.com/mattn/go-oci8"
	// go-sqlite driver
	_ "github.com/mattn/go-sqlite3"
	// ora oracle driver
	_ "gopkg.in/rana/ora.v4"
)

// profiler, see https://golang.org/pkg/net/http/pprof/

// global variables
var _top, _bottom, _search string

// GitVersion defines git version of the server
var GitVersion string

// ServerInfo defines dbs server info
var ServerInfo string

// StartTime represents initial time when we started the server
var StartTime time.Time

// CMSAuth structure to create CMS Auth headers
var CMSAuth cmsauth.CMSAuth

// GraphQLSchema holds graphql schema
var GraphQLSchema *graphql.Schema

// helper function to serve index.html web page
func indexPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

// helper function to use utils.BasePath
func basePath(api string) string {
	return utils.BasePath(Config.Base, api)
}

// helper cuntion to setup all HTTP routes
func handlers() *mux.Router {
	router := mux.NewRouter()
	router.StrictSlash(true) // to allow /route and /route/ end-points

	if Config.MigrationServer {
		router.HandleFunc(basePath("/submit"), MigrationSubmitHandler).Methods("POST")
		router.HandleFunc(basePath("/process"), MigrationProcessHandler).Methods("POST")
		router.HandleFunc(basePath("/remove"), MigrationRemoveHandler).Methods("POST")
		router.HandleFunc(basePath("/status"), MigrationStatusHandler).Methods("GET")
		router.HandleFunc(basePath("/total"), MigrationTotalHandler).Methods("GET")
		router.HandleFunc(basePath("/serverinfo"), ServerInfoHandler).Methods("GET")
	} else if Config.DBSWriterServer {
		router.HandleFunc(basePath("/datatiers"), DatatiersHandler).Methods("POST")
		router.HandleFunc(basePath("/datasetaccesstypes"), DatasetAccessTypesHandler).Methods("POST")
		router.HandleFunc(basePath("/physicsgroups"), PhysicsGroupsHandler).Methods("POST")
		router.HandleFunc(basePath("/datasets"), DatasetsHandler).Methods("POST", "PUT")
		router.HandleFunc(basePath("/blocks"), BlocksHandler).Methods("POST", "PUT")
		router.HandleFunc(basePath("/bulkblocks"), BulkBlocksHandler).Methods("POST")
		router.HandleFunc(basePath("/files"), FilesHandler).Methods("POST", "PUT")
		router.HandleFunc(basePath("/primarydatasets"), PrimaryDatasetsHandler).Methods("POST")
		router.HandleFunc(basePath("/acquisitioneras"), AcquisitionErasHandler).Methods("POST", "PUT")
		router.HandleFunc(basePath("/processingeras"), ProcessingErasHandler).Methods("POST")
		router.HandleFunc(basePath("/outputconfigs"), OutputConfigsHandler).Methods("POST")
		router.HandleFunc(basePath("/fileparents"), FileParentsHandler).Methods("POST")
	} else {
		router.HandleFunc(basePath("/datatiers"), DatatiersHandler).Methods("GET")
		router.HandleFunc(basePath("/datasets"), DatasetsHandler).Methods("GET")
		router.HandleFunc(basePath("/blocks"), BlocksHandler).Methods("GET")
		router.HandleFunc(basePath("/blockTrio"), BlockTrioHandler).Methods("GET")
		router.HandleFunc(basePath("/files"), FilesHandler).Methods("GET")
		router.HandleFunc(basePath("/primarydatasets"), PrimaryDatasetsHandler).Methods("GET")
		router.HandleFunc(basePath("/parentDSTrio"), ParentDSTrioHandler).Methods("GET")
		router.HandleFunc(basePath("/acquisitioneras"), AcquisitionErasHandler).Methods("GET")
		router.HandleFunc(basePath("/releaseversions"), ReleaseVersionsHandler).Methods("GET")
		router.HandleFunc(basePath("/physicsgroups"), PhysicsGroupsHandler).Methods("GET")
		router.HandleFunc(basePath("/primarydstypes"), PrimaryDSTypesHandler).Methods("GET")
		router.HandleFunc(basePath("/datatypes"), DataTypesHandler).Methods("GET")
		router.HandleFunc(basePath("/processingeras"), ProcessingErasHandler).Methods("GET")
		router.HandleFunc(basePath("/outputconfigs"), OutputConfigsHandler).Methods("GET")
		router.HandleFunc(basePath("/datasetaccesstypes"), DatasetAccessTypesHandler).Methods("GET")
		router.HandleFunc(basePath("/runs"), RunsHandler).Methods("GET")
		router.HandleFunc(basePath("/runsummaries"), RunSummariesHandler).Methods("GET")
		router.HandleFunc(basePath("/blockorigin"), BlockOriginHandler).Methods("GET")
		router.HandleFunc(basePath("/blockdump"), BlockDumpHandler).Methods("GET")
		router.HandleFunc(basePath("/blockchildren"), BlockChildrenHandler).Methods("GET")
		router.HandleFunc(basePath("/blockparents"), BlockParentsHandler).Methods("GET")
		router.HandleFunc(basePath("/blocksummaries"), BlockSummariesHandler).Methods("GET")
		router.HandleFunc(basePath("/filechildren"), FileChildrenHandler).Methods("GET")
		router.HandleFunc(basePath("/fileparents"), FileParentsHandler).Methods("GET")
		router.HandleFunc(basePath("/filesummaries"), FileSummariesHandler).Methods("GET")
		router.HandleFunc(basePath("/filelumis"), FileLumisHandler).Methods("GET")
		router.HandleFunc(basePath("/datasetchildren"), DatasetChildrenHandler).Methods("GET")
		router.HandleFunc(basePath("/datasetparents"), DatasetParentsHandler).Methods("GET")
		router.HandleFunc(basePath("/acquisitioneras_ci"), AcquisitionErasCiHandler).Methods("GET")

		router.HandleFunc(basePath("/blockparents"), BlockParentsHandler).Methods("POST")
		router.HandleFunc(basePath("/fileArray"), FileArrayHandler).Methods("POST")
		router.HandleFunc(basePath("/filelumis"), FileLumisHandler).Methods("POST")
		router.HandleFunc(basePath("/datasetlist"), DatasetListHandler).Methods("POST")
		router.HandleFunc(basePath("/fileparentsbylumi"), FileParentsByLumiHandler).Methods("POST")

		// load graphql
		if Config.GraphQLSchema != "" {
			//         schema := dbsGraphQL.InitSchema(Config.GraphQLSchema, dbs.DB)
			//         router.Handle("/query", &relay.Handler{Schema: schema})
			router.HandleFunc(basePath("/query"), QueryHandler).Methods("POST")
		}

		// more complex example
		// https://github.com/gorilla/mux
		//     router.Path(basePath("/dummy")).
		//         Queries("bla", "{bla}").
		//         HandlerFunc(DummyHandler).
		//         Methods("GET")
	}
	// aux APIs used by all DBS servers
	router.HandleFunc(basePath("/status"), StatusHandler).Methods("GET")
	router.HandleFunc(basePath("/serverinfo"), ServerInfoHandler).Methods("GET")
	router.HandleFunc(basePath("/metrics"), MetricsHandler).Methods("GET")
	router.HandleFunc(basePath("/apis"), ApisHandler).Methods("GET")
	router.HandleFunc(basePath("/dummy"), DummyHandler).Methods("GET", "POST")

	// for all requests
	router.Use(logging.LoggingMiddleware)
	// for all requests perform first auth/authz action
	router.Use(authMiddleware)
	// validate all input parameters
	router.Use(validateMiddleware)

	// use limiter middleware to slow down clients
	router.Use(limitMiddleware)

	// get list of defined routes
	router.Walk(walkFunction)

	return router
}

// webRoutes will hold list of defined HTTP routes
var webRoutes []string

// helper function which walk through mux router and collect all available routes
func walkFunction(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
	path, err := route.GetPathTemplate()
	webRoutes = append(webRoutes, path)
	return err
}

// Server represents main web server for DBS service
//gocyclo:ignore
func Server(configFile string) {
	StartTime = time.Now()
	err := ParseConfig(configFile)
	if err != nil {
		log.Fatal(err)
	}
	utils.VERBOSE = Config.Verbose
	utils.STATICDIR = Config.StaticDir
	utils.BASE = Config.Base
	log.SetFlags(0)
	if Config.Verbose > 0 {
		log.SetFlags(log.Lshortfile)
	}
	log.SetOutput(new(logging.LogWriter))
	if Config.LogFile != "" {
		rl, err := rotatelogs.New(Config.LogFile + "-%Y%m%d")
		if err == nil {
			rotlogs := logging.RotateLogWriter{RotateLogs: rl}
			log.SetOutput(rotlogs)
		}
	}
	// initialize logging module
	logging.CMSMonitType = Config.MonitType
	logging.CMSMonitProducer = Config.MonitProducer

	if err != nil {
		log.Printf("Unable to parse, time: %v, config: %v\n", time.Now(), configFile)
	}
	log.Println("Configuration:", Config.String())

	// initialize cmsauth layer
	CMSAuth.Init(Config.Hmac)

	// initialize limiter
	initLimiter(Config.LimiterPeriod)

	// initialize record validator
	dbs.RecordValidator = validator.New()

	// initialize templates
	tmplData := make(map[string]interface{})
	tmplData["Time"] = time.Now()
	//     var templates ServerTemplates
	//     _top = templates.Tmpl(config.Config.Templates, "top.tmpl", tmplData)
	//     _bottom = templates.Tmpl(config.Config.Templates, "bottom.tmpl", tmplData)

	// static handlers
	for _, dir := range []string{"js", "css", "images"} {
		m := fmt.Sprintf("/%s/%s/", Config.Base, dir)
		d := fmt.Sprintf("%s/%s", utils.STATICDIR, dir)
		http.Handle(m, http.StripPrefix(m, http.FileServer(http.Dir(d))))
	}

	// set database connection once
	dbtype, dburi, dbowner := dbs.ParseDBFile(Config.DBFile)
	// for oci driver we know it is oracle backend
	if strings.HasPrefix(dbtype, "oci") {
		utils.ORACLE = true
	}
	db, dberr := sql.Open(dbtype, dburi)
	defer db.Close()
	if dberr != nil {
		log.Fatal(dberr)
	}
	dberr = db.Ping()
	if dberr != nil {
		log.Println("DB ping error", dberr)
	}
	db.SetMaxOpenConns(Config.MaxDBConnections)
	db.SetMaxIdleConns(Config.MaxIdleConnections)
	dbs.DB = db
	dbs.DBTYPE = dbtype

	// load Lexicon patterns
	lexPatterns, err := dbs.LoadPatterns(Config.LexiconFile)
	if err != nil {
		log.Fatal(err)
	}
	dbs.LexiconPatterns = lexPatterns

	// load DBS SQL statements
	dbsql := dbs.LoadSQL(dbowner)
	dbs.DBSQL = dbsql
	dbs.DBOWNER = dbowner

	// migration settings
	dbs.MigrationProcessTimeout = Config.MigrationProcessTimeout
	dbs.MigrationServerInterval = Config.MigrationServerInterval

	// init graphql
	if Config.GraphQLSchema != "" {
		GraphQLSchema = dbsGraphQL.InitSchema(Config.GraphQLSchema, dbs.DB)
	}

	// dynamic handlers
	if Config.CSRFKey != "" {
		CSRF := csrf.Protect(
			[]byte(Config.CSRFKey),
			csrf.RequestHeader("Authenticity-Token"),
			csrf.FieldName("authenticity_token"),
			csrf.Secure(Config.Production),
			csrf.ErrorHandler(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					log.Printf("### CSRF error handler: %+v\n", r)
					w.WriteHeader(http.StatusForbidden)
				},
			)),
		)

		http.Handle("/", CSRF(handlers()))
	} else {
		http.Handle("/", handlers())
	}
	// define our HTTP server
	addr := fmt.Sprintf(":%d", Config.Port)
	server := &http.Server{
		Addr: addr,
	}

	// make extra channel for graceful shutdown
	// https://medium.com/honestbee-tw-engineer/gracefully-shutdown-in-go-http-server-5f5e6b83da5a
	httpDone := make(chan os.Signal, 1)
	signal.Notify(httpDone, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// Start either HTTPs or HTTP web server
		_, e1 := os.Stat(Config.ServerCrt)
		_, e2 := os.Stat(Config.ServerKey)
		if e1 == nil && e2 == nil {
			//start HTTPS server which require user certificates
			rootCA := x509.NewCertPool()
			caCert, _ := ioutil.ReadFile(Config.RootCA)
			rootCA.AppendCertsFromPEM(caCert)
			server = &http.Server{
				Addr: addr,
				TLSConfig: &tls.Config{
					//                 ClientAuth: tls.RequestClientCert,
					RootCAs: rootCA,
				},
			}
			log.Println("Starting HTTPs server", addr)
			err = server.ListenAndServeTLS(Config.ServerCrt, Config.ServerKey)
		} else {
			// Start server without user certificates
			log.Println("Starting HTTP server", addr)
			err = server.ListenAndServe()
		}
		if err != nil {
			log.Printf("Fail to start server %v", err)
		}
	}()

	// start migration server if necessary
	migDone := make(chan bool)
	if Config.MigrationServer {
		go dbs.MigrationServer(dbs.MigrationServerInterval, dbs.MigrationProcessTimeout, migDone)
	}

	// properly stop our HTTP and Migration Servers
	<-httpDone
	log.Print("HTTP server stopped")

	// send notification to stop migration server
	if Config.MigrationServer {
		migDone <- true
	}

	// add extra timeout for shutdown service stuff
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
	}()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
	log.Print("HTTP server exited properly")
}
