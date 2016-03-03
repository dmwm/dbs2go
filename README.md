This is an example project how to write generic Data Base Service in Go.

## Installation instruction for Oracle driver

- Download oracle client libraries and sdk from Oracle web site
- Setup the environment and build ora.v3 package
```
export CGO_CFLAGS=-I/path/Oracle/instantclient_12_1/sdk/include
export CGO_LDFLAGS="-L/path/Oracle/instantclient_12_1/ -locci -lclntsh -lipc1 -lmql1 -lnnz12 -lclntshcore -lons"
go get gopkg.in/rana/ora.v3
```


