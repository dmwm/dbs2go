### dbs2go installation instruction
To build `dbs2go` executable we need the following components:
- [Go compiler](https://golang.org/)
- [ORACLE instant client libs/headers](https://www.oracle.com/database/technologies/instant-client/downloads.html)
- `oci8.pc` appropriate oci file, e.g.
```
libdir=/opt/Oracle/instantclient_19_8/
includedir=/opt/Oracle/instantclient_19_8/sdk/include/

Name: oci8
Description: oci8 library
Version: 19.8
Cflags: -I${includedir}
Libs: -L${libdir} -lclntsh
```
After that, you can build `dbs2go` executable by calling `make`.

#### Additional instructions
`dbs2go` may use either SQLite or ORACLE databases as its backend.
To access ORACLE database we may use either:
- [ora](gopkg.in/rana/ora.v3) driver
- [oci8](https://github.com/mattn/go-oci8) driver

#### Installation instruction for ora Oracle driver

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

