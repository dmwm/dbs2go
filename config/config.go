package config

// configuration module for dbs2go
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

// Configuration stores dbs configuration parameters
type Configuration struct {
	Port             int    `json:"port"`               // dbs port number
	StaticDir        string `json:"staticdir"`          // location of static directory
	Base             string `json:"base"`               // dbs base path
	Verbose          int    `json:"verbose"`            // verbosity level
	LogFile          string `json:"log_file"`           // server log file
	UTC              bool   `json:"utc"`                // report logger time in UTC
	PrintMonitRecord bool   `json:"print_monit_record"` // print monit record on stdout
	Hmac             string `json:"hmac"`               // cmsweb hmac file location

	// db related configuration
	DBFile             string `json:"dbfile"`               // dbs db file with secrets
	MaxDBConnections   int    `json:"max_db_connections"`   // maximum number of DB connections
	MaxIdleConnections int    `json:"max_idle_connections"` // maximum number of idle connections

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
}

// global variables
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
		Config.MaxDBConnections = 100
	}
	if Config.MaxIdleConnections == 0 {
		Config.MaxIdleConnections = 100
	}
	return nil
}
