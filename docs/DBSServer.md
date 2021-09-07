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

# to start DBS Reader server
./dbs2go -config dbs-writer.json

# to start DBS Reader server
./dbs2go -config dbs-migration.json
```
Each configuration file represents `Configuration` struct located in
`web/config.go` file.

### Code logic
The HTTP server consists of the following components:
```
# server.go implements server logic
# handlers.go contains all HTTP handlers
# middleware.go contains all midlewares layers
```
The code relies on the following logic:
```
# get HTTP request and route it to appropriate handler

# handler either calls DBSGetHandler or DBSPostHandler

# the DBSXXXHandler takes api name and route it to DBS API

# the individual DBS API perform validation of input parameters
# check that provided set of parameters consists with required attributes
# and implements appropriate logic of the API, e.g. queyr DBS DB for requested
# information
```
