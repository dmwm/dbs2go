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
package web

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	//     _ "github.com/go-sql-driver/mysql"
	_ "net/http/pprof"

	"github.com/dmwm/cmsauth"
	"github.com/gorilla/csrf"
	"github.com/gorilla/mux"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/config"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
	_ "gopkg.in/rana/ora.v4"
)

// profiler, see https://golang.org/pkg/net/http/pprof/

// global variables
var _top, _bottom, _search string

// StartTime represents initial time when we started the server
var StartTime time.Time

// CMSAuth structure to create CMS Auth headers
var CMSAuth cmsauth.CMSAuth

// helper function to serve index.html web page
func indexPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

// helper function to provide end-point path
func basePath(s string) string {
	if config.Config.Base != "" {
		if strings.HasPrefix(s, "/") {
			s = strings.Replace(s, "/", "", 1)
		}
		if strings.HasPrefix(config.Config.Base, "/") {
			return fmt.Sprintf("%s/%s", config.Config.Base, s)
		}
		return fmt.Sprintf("/%s/%s", config.Config.Base, s)
	}
	return s
}

func handlers() *mux.Router {
	router := mux.NewRouter()

	// visible routes
	router.HandleFunc(basePath("/datatiers"), LoggingHandler(DatatiersHandler)).Methods("GET", "POST")
	router.HandleFunc(basePath("/datasets"), LoggingHandler(DatasetsHandler)).Methods("GET", "POST")
	router.HandleFunc(basePath("/blocks"), LoggingHandler(BlocksHandler)).Methods("GET", "POST")
	router.HandleFunc(basePath("/bulkblocks"), LoggingHandler(BulkBlocksHandler)).Methods("POST")
	router.HandleFunc(basePath("/files"), LoggingHandler(FilesHandler)).Methods("GET", "POST")
	router.HandleFunc(basePath("/primarydatasets"), LoggingHandler(PrimaryDatasetsHandler)).Methods("GET", "POST")
	router.HandleFunc(basePath("/primdstypes"), LoggingHandler(PrimaryDSTypesHandler)).Methods("GET", "POST")

	// Not implemented APIs
	router.HandleFunc(basePath("/acquisitioneras"), LoggingHandler(AcquisitionErasHandler)).Methods("GET")
	router.HandleFunc(basePath("/releaseversions"), LoggingHandler(ReleaseVersionsHandler)).Methods("GET")
	router.HandleFunc(basePath("/physicsgroups"), LoggingHandler(PhysicsGroupsHandler)).Methods("GET")
	router.HandleFunc(basePath("/primarydstypes"), LoggingHandler(PrimaryDSTypesHandler)).Methods("GET")
	router.HandleFunc(basePath("/datatypes"), LoggingHandler(DataTypesHandler)).Methods("GET")
	router.HandleFunc(basePath("/processingeras"), LoggingHandler(ProcessingErasHandler)).Methods("GET")
	router.HandleFunc(basePath("/outputconfigs"), LoggingHandler(OutputConfigsHandler)).Methods("GET")
	router.HandleFunc(basePath("/datasetaccesstypes"), LoggingHandler(DatasetAccessTypesHandler)).Methods("GET")

	router.HandleFunc(basePath("/runs"), LoggingHandler(RunsHandler)).Methods("GET")
	router.HandleFunc(basePath("/runsummaries"), LoggingHandler(RunSummariesHandler)).Methods("GET")

	router.HandleFunc(basePath("/blockorigin"), LoggingHandler(BlockOriginHandler)).Methods("GET")
	router.HandleFunc(basePath("/blockTrio"), LoggingHandler(DummyHandler)).Methods("GET")
	router.HandleFunc(basePath("/blockdump"), LoggingHandler(DummyHandler)).Methods("GET")
	router.HandleFunc(basePath("/blockchildren"), LoggingHandler(BlockChildrenHandler)).Methods("GET")
	router.HandleFunc(basePath("/blockparents"), LoggingHandler(BlockParentsHandler)).Methods("GET", "POST")

	router.HandleFunc(basePath("/filechildren"), LoggingHandler(FileChildrenHandler)).Methods("GET")
	router.HandleFunc(basePath("/fileparents"), LoggingHandler(FileParentsHandler)).Methods("GET")
	router.HandleFunc(basePath("/filesummaries"), LoggingHandler(FileSummariesHandler)).Methods("GET")
	router.HandleFunc(basePath("/filelumis"), LoggingHandler(FileLumisHandler)).Methods("GET", "POST")
	router.HandleFunc(basePath("/datasetchildren"), LoggingHandler(DatasetChildrenHandler)).Methods("GET")
	router.HandleFunc(basePath("/datasetparents"), LoggingHandler(DatasetParentsHandler)).Methods("GET")
	router.HandleFunc(basePath("/parentDSTrio"), LoggingHandler(ParentDSTrioHandler)).Methods("GET")
	router.HandleFunc(basePath("/acquisitioneras_ci"), LoggingHandler(AcquisitionErasCiHandler)).Methods("GET")

	// POST routes
	router.HandleFunc(basePath("/fileArray"), LoggingHandler(FileArrayHandler)).Methods("POST")
	router.HandleFunc(basePath("/datasetlist"), LoggingHandler(DatasetListHandler)).Methods("POST")
	router.HandleFunc(basePath("/fileparentsbylumi"), LoggingHandler(FileParentsByLumiHandler)).Methods("POST")

	// aux APIs
	router.HandleFunc(basePath("/status"), StatusHandler).Methods("GET")
	router.HandleFunc(basePath("/metrics"), MetricsHandler).Methods("GET")
	router.HandleFunc(basePath("/dummy"), LoggingHandler(DummyHandler)).Methods("GET", "POST")

	// more complex example
	// https://github.com/gorilla/mux
	//     router.Path(basePath("/dummy")).
	//         Queries("bla", "{bla}").
	//         HandlerFunc(LoggingHandler(DummyHandler)).
	//         Methods("GET")

	// for all requests perform first auth/authz action
	router.Use(authMiddleware)
	// validate all input parameters
	router.Use(validateMiddleware)

	return router
}

// Server represents main web server for DBS service
func Server(configFile string) {
	StartTime = time.Now()
	err := config.ParseConfig(configFile)
	utils.VERBOSE = config.Config.Verbose
	utils.STATICDIR = config.Config.StaticDir
	log.SetFlags(0)
	if config.Config.Verbose > 0 {
		log.SetFlags(log.Lshortfile)
	}
	log.SetOutput(new(logWriter))
	if config.Config.LogFile != "" {
		rl, err := rotatelogs.New(config.Config.LogFile + "-%Y%m%d")
		if err == nil {
			rotlogs := rotateLogWriter{RotateLogs: rl}
			log.SetOutput(rotlogs)
		}
	}
	if err != nil {
		log.Printf("Unable to parse, time: %v, config: %v\n", time.Now(), configFile)
	}
	log.Println("Configuration:", config.Config.String())

	// initialize cmsauth layer
	CMSAuth.Init(config.Config.Hmac)

	// initialize templates
	tmplData := make(map[string]interface{})
	tmplData["Time"] = time.Now()
	//     var templates ServerTemplates
	//     _top = templates.Tmpl(config.Config.Templates, "top.tmpl", tmplData)
	//     _bottom = templates.Tmpl(config.Config.Templates, "bottom.tmpl", tmplData)

	// static handlers
	for _, dir := range []string{"js", "css", "images"} {
		m := fmt.Sprintf("/%s/%s/", config.Config.Base, dir)
		d := fmt.Sprintf("%s/%s", utils.STATICDIR, dir)
		http.Handle(m, http.StripPrefix(m, http.FileServer(http.Dir(d))))
	}

	// set database connection once
	dbtype, dburi, dbowner := dbs.ParseDBFile(config.Config.DBFile)
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
	db.SetMaxOpenConns(config.Config.MaxDBConnections)
	db.SetMaxIdleConns(config.Config.MaxIdleConnections)
	dbs.DB = db
	dbs.DBTYPE = dbtype

	// load DBS SQL statements
	dbsql := dbs.LoadSQL(dbowner)
	dbs.DBSQL = dbsql
	dbs.DBOWNER = dbowner

	// dynamic handlers
	if config.Config.CSRFKey != "" {
		CSRF := csrf.Protect(
			[]byte(config.Config.CSRFKey),
			csrf.RequestHeader("Authenticity-Token"),
			csrf.FieldName("authenticity_token"),
			csrf.Secure(config.Config.Production),
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

	// Start server
	addr := fmt.Sprintf(":%d", config.Config.Port)
	_, e1 := os.Stat(config.Config.ServerCrt)
	_, e2 := os.Stat(config.Config.ServerKey)
	if e1 == nil && e2 == nil {
		//start HTTPS server which require user certificates
		rootCA := x509.NewCertPool()
		caCert, _ := ioutil.ReadFile(config.Config.RootCA)
		rootCA.AppendCertsFromPEM(caCert)
		server := &http.Server{
			Addr: addr,
			TLSConfig: &tls.Config{
				//                 ClientAuth: tls.RequestClientCert,
				RootCAs: rootCA,
			},
		}
		log.Println("Starting HTTPs server", addr)
		err = server.ListenAndServeTLS(config.Config.ServerCrt, config.Config.ServerKey)
	} else {
		// Start server without user certificates
		log.Println("Starting HTTP server", addr)
		err = http.ListenAndServe(addr, nil)
	}
	if err != nil {
		log.Printf("Fail to start server %v", err)
	}
}
