package config

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
	Port      int      `json:"port"`      // dbs port number
	StaticDir string   `json:"staticdir"` // location of static directory
	Hkey      string   `json:"hkey"`      // dbs HKEY file
	Base      string   `json:"base"`      // dbs base path
	DBFile    string   `json:"dbfile"`    // dbs db file with secrets
	Views     []string `json:"views"`     // list of supported views
	Verbose   int      `json:"verbose"`   // verbosity level
	UpdateDNs int      `json:"updateDNs"` // interval in minutes to update user DNs
	LogFile   string   `json:"log_file"`  // server log file

	// server parts
	Templates string `json:"templates"` // location of server templates
	Jscripts  string `json:"jscripts"`  // location of server JavaScript files
	Images    string `json:"images"`    // location of server images
	Styles    string `json:"styles"`    // location of server CSS styles

	// server HTTPs parts
	ServerKey        string `json:"serverkey"`          // server key for https
	ServerCrt        string `json:"servercrt"`          // server certificate for https
	RootCA           string `json:"rootCA"`             // RootCA file
	CSRFKey          string `json:"csrfKey"`            // CSRF 32-byte-long-auth-key
	Production       bool   `json:"production"`         // production server or not
	UTC              bool   `json:"utc"`                // report logger time in UTC
	PrintMonitRecord bool   `json:"print_monit_record"` // print monit record on stdout
}

// global variables
var Config Configuration

// String returns string representation of dbs Config
func (c *Configuration) String() string {
	return fmt.Sprintf("<Config port=%d staticdir=%s hkey=%s base=%s dbfile=%s views=%v updateDNs=%d crt=%s key=%s>", c.Port, c.StaticDir, c.Hkey, c.Base, c.DBFile, c.Views, c.UpdateDNs, c.ServerCrt, c.ServerKey)
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
	return nil
}
