package main

import (
	"flag"
	"utils"
	"web"
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
	web.Server(afile, dbfile, base, port, sdir)
}
