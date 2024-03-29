// dbs2go - An example code how to write data-base based app
//
// Copyright (c) 2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/dmwm/dbs2go/web"
)

// version of the code
var gitVersion string

// Info function returns version string of the server
func info() string {
	goVersion := runtime.Version()
	tstamp := time.Now().Format("2006-02-01")
	return fmt.Sprintf("dbs2go git=%s go=%s date=%s", gitVersion, goVersion, tstamp)
}

func main() {
	var config string
	flag.StringVar(&config, "config", "config.json", "dbs2go config file")
	var version bool
	flag.BoolVar(&version, "version", false, "Show version")
	flag.Parse()
	if version {
		fmt.Println(info())
		os.Exit(0)

	}
	web.GitVersion = gitVersion
	web.ServerInfo = info()
	web.Server(config)
}
