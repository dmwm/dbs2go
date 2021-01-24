// dbs2go - An example code how to write data-base based app
//
// Copyright (c) 2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/vkuznet/dbs2go/web"
)

func main() {
	var config string
	flag.StringVar(&config, "config", "config.json", "dbs2go config file")
	var version bool
	flag.BoolVar(&version, "version", false, "Show version")
	flag.Parse()
	if version {
		fmt.Println(web.Info())
		os.Exit(0)

	}
	web.Server(config)
}
