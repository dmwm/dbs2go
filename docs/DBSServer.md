### DBS Server
The DBS Server consists of several components:

- [DBS Reader](DBSReader.md) which provides DBS read APIs
- [DBS Writer](DBSWriter.md) which handles DBS write APIs and writes data to underlying DBS DB
- [DBS Migration server](MigrationServer.md)

Each server can be run via `dbs2go` executable with appropriate configuration
file, e.g.

```
# to start DBS Reader server
./dbs2go -config dbs-reader.json
```
Please refer to `Configuration` struct located in `web/config.go` file for more
details of each DBS server configuration option.

Here is architecture of the DBS server:
![DBS Server Architecture](images/DBSServer.png)

### Repository structure and code logic
[dns2go](https://github.com/vkuznet/dbs2go) has the following structure:
- [dbs](https://github.com/vkuznet/dbs2go/tree/master/dbs)
  folder contains all business and DAO objects
- [static](https://github.com/vkuznet/dbs2go/tree/master/static) area contains 
  - [SQL templates](https://github.com/vkuznet/dbs2go/tree/master/static/sql)
  - [HTPP templates](https://github.com/vkuznet/dbs2go/tree/master/static/templates)
  - [CSS](https://github.com/vkuznet/dbs2go/tree/master/static/css)
  - [DB schemas](https://github.com/vkuznet/dbs2go/tree/master/static/schema)
  - [images](https://github.com/vkuznet/dbs2go/tree/master/static/images)
  - [lexicon.json](https://github.com/vkuznet/dbs2go/blob/master/static/lexicon.json)
  to store regular expression for validating input parameters
- [web](https://github.com/vkuznet/dbs2go/tree/master/web) contains all
  codebase related to HTTP web server, including handlers, middleware
  implementaions, etc.
- [utils](https://github.com/vkuznet/dbs2go/tree/master/utils) contains general
  utilities used across the codebase
- [graphql](https://github.com/vkuznet/dbs2go/tree/master/graphql) contains
  initial implementation of [GraphQL](https://graphql.org/) for DBS server.

The HTTP server consists of the following components:
- [server.go](https://github.com/vkuznet/dbs2go/blob/master/web/server.go)
  implements server logic
- [handlers.go](https://github.com/vkuznet/dbs2go/blob/master/web/handlers.go)
  contains all HTTP handlers
- [middleware.go](https://github.com/vkuznet/dbs2go/blob/master/web/middleware.go)
  contains all midlewares layers
- [metrics.go](https://github.com/vkuznet/dbs2go/blob/master/web/templates.go)
  contains implementation of DBS metrics which can be used by
  [Prometheus](https://prometheus.io/)
- [templates.go](https://github.com/vkuznet/dbs2go/blob/master/web/templates.go)
  holds implementation of DBS templates

The HTTP server relies on the following logic:
- get HTTP request and route it to appropriate handler
- handler either calls DBSGetHandler or DBSPostHandler
- the DBSXXXHandler takes api name and route it to DBS API
- the individual DBS API perform validation of input parameters
- check that provided set of parameters consists with required attributes
- and implements appropriate logic of the API, e.g. queyr DBS DB for requested
- information

### DBS business and DAO logic
The DBS business and DAO logic resides within
[dbs](https://github.com/vkuznet/dbs2go/blob/master/dbs) folder.
The individual file, e.g.
[tiers.go](https://github.com/vkuznet/dbs2go/blob/master/dbs/tiers.go)
holds full implemenation for that specific api `/tiers` used by DBS server.
It includes a `DataTiers` DBS API, a `DataTiers` struct representing
associative DBS Table, Each DBS API implements `DBRecord` interface (found in
[dbs/dbs.go](https://github.com/vkuznet/dbs2go/blob/master/dbs/dbs.go)):
```
type DBRecord interface {
	Insert(tx *sql.Tx) error
	Validate() error
	SetDefaults()
	Decode(r io.Reader) error
}
```
The `DBRecord` can be inserted to DB via `Insert` API, it can be
validated via `Validatae` method, it can implement defaults via
`SetDefaults` method and can be decoded via `Decode` API. Therefore,
the `/tiers` DBS API, representing by `dbs/tiers.go` codebase contains
`DataTiers` struct which implements the above interface for `DataTiers`
table, i.e. it allows to insert, validate, set defaults and decode
records representing `DataTiers` data.
