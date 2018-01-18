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

	"github.com/vkuznet/dbs2go/utils"
	"github.com/vkuznet/dbs2go/web"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "8989", "server port number")
	//     var afile string
	//     flag.StringVar(&afile, "afile", "", "authentication key file")
	var dbfile string
	flag.StringVar(&dbfile, "dbfile", "", "db file which provides 'dbtype dburi'")
	var base string
	flag.StringVar(&base, "base", "dbs", "base url")
	var sdir string
	flag.StringVar(&sdir, "sdir", "static", "location of static area")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	var https bool
	flag.BoolVar(&https, "https", false, "Start https server")
	var version bool
	flag.BoolVar(&version, "version", false, "Show version")
	flag.Parse()
	if version {
		fmt.Println(info())
		os.Exit(0)

	}
	utils.VERBOSE = verbose
	utils.STATICDIR = sdir
	web.Server(dbfile, base, port, https)
}

// helper function to return current version
func info() string {
	goVersion := runtime.Version()
	tstamp := time.Now()
	return fmt.Sprintf("Build: git={{VERSION}} go=%s date=%s", goVersion, tstamp)
}
