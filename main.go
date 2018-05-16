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

	"github.com/vkuznet/dbs2go/web"
)

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
	web.Server(config)
}

// helper function to return current version
func info() string {
	goVersion := runtime.Version()
	tstamp := time.Now()
	return fmt.Sprintf("Build: git=v00.00.11 go=%s date=%s", goVersion, tstamp)
}
