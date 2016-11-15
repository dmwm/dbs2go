// dbs2go - An example code how to write data-base based app
//
// Copyright (c) 2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"flag"
	"github.com/vkuznet/dbs2go/utils"
	"github.com/vkuznet/dbs2go/web"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "8989", "server port number")
	var afile string
	flag.StringVar(&afile, "afile", "", "authentication key file")
	var dbfile string
	flag.StringVar(&dbfile, "dbfile", "", "db file which provides 'dbtype dburi'")
	var base string
	flag.StringVar(&base, "base", "dbs", "base url")
	var sdir string
	flag.StringVar(&sdir, "sdir", "static", "location of static area")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	flag.Parse()
	utils.VERBOSE = verbose
	utils.STATICDIR = sdir
	web.Server(afile, dbfile, base, port)
}
