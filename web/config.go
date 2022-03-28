package web

// configuration module for dbs2go
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

// Configuration stores dbs configuration parameters
type Configuration struct {
	Port            int      `json:"port"`              // dbs port number
	StaticDir       string   `json:"staticdir"`         // location of static directory
	Base            string   `json:"base"`              // dbs base path
	Verbose         int      `json:"verbose"`           // verbosity level
	LogFile         string   `json:"log_file"`          // server log file (should ends with .log) or log area
	UTC             bool     `json:"utc"`               // report logger time in UTC
	MonitType       string   `json:"monit_type"`        // monit record type
	MonitProducer   string   `json:"monit_producer"`    // monit record producer
	Hmac            string   `json:"hmac"`              // cmsweb hmac file location
	LimiterPeriod   string   `json:"limiter_rate"`      // limiter rate value
	LimiterHeader   string   `json:"limiter_header"`    // limiter header to use
	LimiterSkipList []string `json:"limiter_skip_list"` // limiter skip list
	MetricsPrefix   string   `json:"metrics_prefix"`    // metrics prefix used for prometheus
	ServerType      string   `json:"server_type"`       // DBS server type to start: DBSReader, DBSWriter, DBSMigrate, DBSMigration
	Etag            string   `json:"etag"`              // etag value to use for ETag generation
	CacheControl    string   `json:"cache_control"`     // Cache-Control value, e.g. max-age=300
	CMSRole         string   `json:"cms_role"`          // cms role for write access
	CMSGroup        string   `json:"cms_group"`         // cms group for write access

	// Migration server settings
	MigrationDBFile          string `json:"migration_dbfile"`           // dbfile with secrets
	MigrationServerInterval  int    `json:"migration_server_interval"`  // migration process interval
	MigrationProcessTimeout  int    `json:"migration_process_timeout"`  // migration process timeout
	MigrationCleanupInterval int    `json:"migration_cleanup_interval"` // migration cleanup interval
	MigrationCleanupOffset   int64  `json:"migration_cleanup_offset"`   // migration cleanup offset

	// db related configuration
	DBFile               string `json:"dbfile"`                  // dbs db file with secrets
	MaxDBConnections     int    `json:"max_db_connections"`      // maximum number of DB connections
	MaxIdleConnections   int    `json:"max_idle_connections"`    // maximum number of idle connections
	DBMonitoringInterval int    `json:"db_monitoring_interval"`  // db mon interval in seconds
	LexiconFile          string `json:"lexicon_file"`            // lexicon json file
	FileChunkSize        int    `json:"file_chunk_size"`         // chunk size for []File insertion
	FileLumiChunkSize    int    `json:"file_lumi_chunk_size"`    // chunk size for []FileLumi insertion
	FileLumiMaxSize      int    `json:"file_lumi_max_size"`      // max size for []FileLumi insertion
	FileLumiInsertMethod string `json:"file_lumi_insert_method"` // insert method for FileLumi list
	ConcurrentBulkBlocks bool   `json:"concurrent_bulkblocks"`   // use concurrent BulkBlocks API

	// server static parts
	Templates string `json:"templates"` // location of server templates
	Jscripts  string `json:"jscripts"`  // location of server JavaScript files
	Images    string `json:"images"`    // location of server images
	Styles    string `json:"styles"`    // location of server CSS styles

	// security parts
	ServerKey  string `json:"serverkey"`  // server key for https
	ServerCrt  string `json:"servercrt"`  // server certificate for https
	RootCA     string `json:"rootCA"`     // RootCA file
	CSRFKey    string `json:"csrfKey"`    // CSRF 32-byte-long-auth-key
	Production bool   `json:"production"` // production server or not

	// GraphQL parts
	GraphQLSchema string `json:"graphqlSchema"` // graph ql schema file name
}

// Config represents global configuration object
var Config Configuration

// String returns string representation of dbs Config
func (c *Configuration) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		log.Println("ERROR: fail to marshal configuration", err)
		return ""
	}
	return string(data)
}

// ParseConfig parses given configuration file and initialize Config object
func ParseConfig(configFile string) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("unable to read config file", configFile, err)
		return err
	}
	err = json.Unmarshal(data, &Config)
	if err != nil {
		log.Println("unable to parse config file", configFile, err)
		return err
	}
	if Config.MaxDBConnections == 0 {
		Config.MaxDBConnections = 1000
	}
	if Config.MaxIdleConnections == 0 {
		Config.MaxIdleConnections = 100
	}
	if Config.LimiterPeriod == "" {
		Config.LimiterPeriod = "100-S"
	}
	if Config.MigrationProcessTimeout == 0 {
		Config.MigrationProcessTimeout = 300 // in seconds
	}
	if Config.MigrationServerInterval == 0 {
		Config.MigrationServerInterval = 60 // in seconds
	}
	if Config.MigrationCleanupInterval == 0 {
		Config.MigrationCleanupInterval = 600 // in seconds
	}
	if Config.MigrationCleanupOffset == 0 {
		Config.MigrationCleanupOffset = 3 * 30 * 24 * 60 * 60 // 3 months in seconds
	}
	if Config.MetricsPrefix == "" {
		Config.MetricsPrefix = "dbs2go"
	}
	// keep reasonable chunk/max sizes such that in total we'll have
	// around few hundreds goroutines running at runtime, e.g.
	// 10 files x 20 file-lumis (10000/500) = 200 goroutines
	if Config.FileChunkSize == 0 {
		Config.FileChunkSize = 10
	}
	if Config.FileLumiChunkSize == 0 {
		Config.FileLumiChunkSize = 500
	}
	if Config.FileLumiMaxSize == 0 {
		Config.FileLumiMaxSize = 10000
	}
	if Config.FileLumiInsertMethod == "" {
		// possible values are: temptable, chunks, linear
		Config.FileLumiInsertMethod = "chunks"
	}
	if Config.Templates == "" {
		Config.Templates = fmt.Sprintf("%s/templates", Config.StaticDir)
	}
	return nil
}
