package config

// configuration module for dbs2go
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	logs "github.com/sirupsen/logrus"
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
	ServerKey string   `json:"serverkey"` // server key for https
	ServerCrt string   `json:"servercrt"` // server certificate for https
	UpdateDNs int      `json:"updateDNs"` // interval in minutes to update user DNs
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
		logs.WithFields(logs.Fields{"configFile": configFile}).Fatal("Unable to read", err)
		return err
	}
	err = json.Unmarshal(data, &Config)
	if err != nil {
		logs.WithFields(logs.Fields{"configFile": configFile}).Fatal("Unable to parse", err)
		return err
	}
	return nil
}
