### dbs2go package

[![Build Status](https://travis-ci.org/vkuznet/dbs2go.svg?branch=master)](https://travis-ci.org/vkuznet/dbs2go)
[![GoDoc](https://godoc.org/github.com/vkuznet/dbs2go?status.svg)](https://godoc.org/github.com/vkuznet/dbs2go)

This is an example project how to write generic Data Base Service in Go.

### Installation instruction for ora Oracle driver

- Download oracle client libraries and sdk from Oracle web site
- Setup the environment and build ora.v3 package
```
export CGO_CFLAGS=-I/path/Oracle/instantclient_12_1/sdk/include
# for Linux
export CGO_LDFLAGS="-L/path/Oracle/instantclient_12_1/ -locci -lclntsh -lipc1 -lmql1 -lnnz12 -lclntshcore -lons"
# for OSX
export CGO_LDFLAGS="-L/opt/Oracle/instantclient_11_2/ -locci -lclntsh -lnnz11"
go get gopkg.in/rana/ora.v3
```

To use this driver we define dbfile with *ora* entry
```
ora oracleLogin/oraclePassword@DB
```


### Installation instructions for oci8 Oracle driver
We can use another driver ```https://github.com/mattn/go-oci8```
To install it, we need to create an oci8.pc file and put it elsewhere.
Then we must point *PKG_CONFIG_PATH* environment variable to location of
oci8.pc directory. Here is an example of oci8.pc driver

```
libdir=/path/Oracle/instantclient_12_1/
includedir=/path/Oracle/instantclient_12_1/sdk/include/

Name: oci8
Description: oci8 library
Version: 12.1
Cflags: -I${includedir}
Libs: -L${libdir} -lclntsh
```

To install the driver we simply do

```
go get github.com/mattn/go-oci8
```

To use this driver we define dbfile with *oci8* entry
```
oci8 oracleLogin/oraclePassword@DB
```

### Code logic
The HTTP server consists of the following components:
```
# server.go implements server logic
# handlers.go contains all HTTP handlers
# middleware.go contains all midlewares layers
```
We follow this logic:
```
# get HTTP request and route it to appropriate handler

# handler either calls DBSGetHandler or DBSPostHandler

# the DBSXXXHandler takes api name and route it to DBS API

# the individual DBS API perform validation of input parameters
# check that provided set of parameters consists with required attributes
# and implements appropriate logic of the API, e.g. queyr DBS DB for requested
# information
```
