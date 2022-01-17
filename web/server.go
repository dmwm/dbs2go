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

	if Config.ServerType == "DBSMigrate" {
		router.HandleFunc(basePath("/submit"), MigrationSubmitHandler).Methods("POST")
		router.HandleFunc(basePath("/process"), MigrationProcessHandler).Methods("POST")
		router.HandleFunc(basePath("/cancel"), MigrationCancelHandler).Methods("POST")
		router.HandleFunc(basePath("/remove"), MigrationRemoveHandler).Methods("POST")
		router.HandleFunc(basePath("/status"), MigrationStatusHandler).Methods("GET")
		router.HandleFunc(basePath("/total"), MigrationTotalHandler).Methods("GET")
		router.HandleFunc(basePath("/blocks"), BlocksHandler).Methods("GET")
		router.HandleFunc(basePath("/bulkblocks"), BulkBlocksHandler).Methods("POST")
		router.HandleFunc(basePath("/blockparents"), BlocksHandler).Methods("GET")
		router.HandleFunc(basePath("/datasetparents"), DatasetParentsHandler).Methods("GET")
	} else if Config.ServerType == "DBSMigration" {
		router.HandleFunc(basePath("/blocks"), BlocksHandler).Methods("POST", "PUT", "GET")
		router.HandleFunc(basePath("/bulkblocks"), BulkBlocksHandler).Methods("POST")
		router.HandleFunc(basePath("/status"), StatusHandler).Methods("GET")
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

		router.HandleFunc(basePath("/dbstats"), DBStatsHandler).Methods("GET")
		router.HandleFunc(basePath("/status"), StatusHandler).Methods("GET")

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

	// add DBS writer APIs
	if Config.ServerType == "DBSWriter" {
		router.HandleFunc(basePath("/datatiers"), DatatiersHandler).Methods("POST", "GET")
		router.HandleFunc(basePath("/datasets"), DatasetsHandler).Methods("POST", "PUT", "GET")
		router.HandleFunc(basePath("/blocks"), BlocksHandler).Methods("POST", "PUT", "GET")
		router.HandleFunc(basePath("/bulkblocks"), BulkBlocksHandler).Methods("POST")
		router.HandleFunc(basePath("/files"), FilesHandler).Methods("POST", "PUT", "GET")
		router.HandleFunc(basePath("/physicsgroups"), PhysicsGroupsHandler).Methods("POST")
		router.HandleFunc(basePath("/datasetaccesstypes"), DatasetAccessTypesHandler).Methods("POST", "GET")
		router.HandleFunc(basePath("/primarydatasets"), PrimaryDatasetsHandler).Methods("POST", "GET")
		router.HandleFunc(basePath("/acquisitioneras"), AcquisitionErasHandler).Methods("POST", "PUT", "GET")
		router.HandleFunc(basePath("/processingeras"), ProcessingErasHandler).Methods("POST", "GET")
		router.HandleFunc(basePath("/outputconfigs"), OutputConfigsHandler).Methods("POST", "GET")
		router.HandleFunc(basePath("/fileparents"), FileParentsHandler).Methods("POST", "GET")
		router.HandleFunc(basePath("/fileparentsbylumi"), FileParentsByLumiHandler).Methods("POST", "GET")
		router.HandleFunc(basePath("/blockparents"), BlockParentsHandler).Methods("GET")
		router.HandleFunc(basePath("/datasetparents"), DatasetParentsHandler).Methods("GET")
	}

	// aux APIs used by all DBS servers
	router.HandleFunc(basePath("/healthz"), StatusHandler).Methods("GET")
	router.HandleFunc(basePath("/serverinfo"), ServerInfoHandler).Methods("GET")
	router.HandleFunc(basePath("/metrics"), MetricsHandler).Methods("GET")
	router.HandleFunc(basePath("/apis"), ApisHandler).Methods("GET")
	// backward compatible with Python server
	router.HandleFunc(basePath("/help"), ApisHandler).Methods("GET")
	router.HandleFunc(basePath("/dummy"), DummyHandler).Methods("GET", "POST")

	// for all requests
	router.Use(headerMiddleware)
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

// webRoutes will HTTP routes
var webRoutes map[string][]string

// helper function which walk through mux router and collect all available routes
func walkFunction(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
	if webRoutes == nil {
		webRoutes = make(map[string][]string)
	}
	path, err := route.GetPathTemplate()
	if err != nil {
		log.Println("unable to get route path templates", err)
		return err
	}
	methods, err := route.GetMethods()
	if err != nil {
		log.Println("unable to get route methds", err)
		return err
	}
	if v, ok := webRoutes[path]; ok {
		v = append(v, methods...)
		webRoutes[path] = v
	} else {
		webRoutes[path] = methods
	}
	return err
}

// helper function to initialize DB access
func dbInit(dbtype, dburi string) (*sql.DB, error) {
	//     close existing DB connection if it exist
	//     if pdb != nil {
	//         pdb.Close()
	//     }
	db, dberr := sql.Open(dbtype, dburi)
	if dberr != nil {
		log.Printf("unable to open %s %s, error %v", dbtype, dburi)
		return nil, dberr
	}
	dberr = db.Ping()
	if dberr != nil {
		log.Println("DB ping error", dberr)
		return nil, dberr
	}
	db.SetMaxOpenConns(Config.MaxDBConnections)
	db.SetMaxIdleConns(Config.MaxIdleConnections)
	return db, nil
}

// helper function to perform db connection monitoring
// it should be used as goroutine in main server
func dbMonitor(dbtype, dburi string, interval int) {
	for {
		// get some results from DB
		err := dbs.GetTestData()
		if err != nil {
			// if we get ORA error we should restart DB connection
			log.Println("unable to get test data query, error", err)
			if dbs.DB != nil {
				dbs.DB.Close()
			}
			db, dberr := dbInit(dbtype, dburi)
			if dberr != nil {
				log.Println("unable to init DB access, error", dberr)
			}
			dbs.DB = db
		}
		time.Sleep(time.Duration(interval) * time.Second)
	}
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
	utils.Localhost = fmt.Sprintf("http://localhost:%d", Config.Port)
	log.SetFlags(0)
	if Config.Verbose > 0 {
		log.SetFlags(log.Lshortfile)
	}
	log.SetOutput(new(logging.LogWriter))
	if Config.LogFile != "" {
		logName := Config.LogFile
		hostname := os.Getenv("HOSTNAME")
		if hostname == "" {
			hostname, err = os.Hostname()
			if err != nil {
				hostname = "localhost"
			}
		}
		if strings.HasSuffix(logName, ".log") {
			logName = fmt.Sprintf("%s-%s.log", strings.Split(logName, ".log")[0], hostname)
		} else {
			// it is log dir
			logName = fmt.Sprintf("%s/%s.log", logName, hostname)
		}
		logName = strings.Replace(logName, "//", "/", -1)
		//         rl, err := rotatelogs.New(Config.LogFile + "-%Y%m%d")
		rl, err := rotatelogs.New(logName + "-%Y%m%d")
		if err == nil {
			rotlogs := logging.RotateLogWriter{RotateLogs: rl}
			log.SetOutput(rotlogs)
		} else {
			log.Println("ERROR: unable to get rotatelogs", err)
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

	// set configuration for []FileLumi insertion
	dbs.FileChunkSize = Config.FileChunkSize
	dbs.FileLumiChunkSize = Config.FileLumiChunkSize
	dbs.FileLumiMaxSize = Config.FileLumiMaxSize
	dbs.FileLumiInsertMethod = Config.FileLumiInsertMethod

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
	log.Println("parse Config.DBFile:", Config.DBFile)
	dbtype, dburi, dbowner := dbs.ParseDBFile(Config.DBFile)
	// for oci driver we know it is oracle backend
	if strings.HasPrefix(dbtype, "oci") {
		utils.ORACLE = true
	}
	db, dberr := dbInit(dbtype, dburi)
	if dberr != nil {
		log.Fatal(dberr)
	}
	dbs.DB = db
	dbs.DBTYPE = dbtype
	defer dbs.DB.Close()

	// setup MigrationDB access
	if Config.ServerType == "DBSMigration" || Config.ServerType == "DBSMigrate" {
		log.Println("parse Config.MigrationDBFile:", Config.MigrationDBFile)
		dbtype, dburi, dbowner = dbs.ParseDBFile(Config.MigrationDBFile)
		db, dberr = dbInit(dbtype, dburi)
		if dberr != nil {
			log.Fatal(dberr)
		}
		dbs.MigrationDB = db
		defer dbs.MigrationDB.Close()
	}

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
	dbs.MigrationCleanupInterval = Config.MigrationCleanupInterval
	dbs.MigrationCleanupOffset = Config.MigrationCleanupOffset

	// DBS bulkblocks API
	dbs.ConcurrentBulkBlocks = Config.ConcurrentBulkBlocks

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

	// start necessary HTTP servers
	// DBSReader: HTTP server to provide read APIs
	// DBSWriter: HTTP server to provide write APIs
	// DBSMigrate: HTTP server to provide migration APIs
	// DBSMigration server will have two processes:
	// - HTTP server to access and write to internal DB
	// - daemon to process migration requests

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
			log.Printf("Starting %s HTTPs server at %v", Config.ServerType, addr)
			err = server.ListenAndServeTLS(Config.ServerCrt, Config.ServerKey)
		} else {
			// Start server without user certificates
			log.Printf("Starting %s HTTP server at %s", Config.ServerType, addr)
			err = server.ListenAndServe()
		}
		if err != nil {
			log.Printf("Fail to start server %v", err)
		}
	}()

	// star db monitoring goroutine
	if Config.DBMonitoringInterval > 0 {
		go dbMonitor(dbtype, dburi, Config.DBMonitoringInterval)
	}

	migDone := make(chan bool)
	if Config.ServerType == "DBSMigration" {
		go dbs.MigrationServer(dbs.MigrationServerInterval, dbs.MigrationProcessTimeout, migDone)
		go dbs.MigrationCleanupServer(dbs.MigrationCleanupInterval, dbs.MigrationCleanupOffset, migDone)
	}

	// properly stop our HTTP and Migration Servers
	<-httpDone
	log.Print("HTTP server stopped")

	// close database connection pointer
	if dbs.DB != nil {
		dbs.DB.Close()
	}

	// close database connection pointer
	if dbs.MigrationDB != nil {
		dbs.MigrationDB.Close()
	}

	// send notification to stop migration server
	if Config.ServerType == "DBSMigration" {
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
