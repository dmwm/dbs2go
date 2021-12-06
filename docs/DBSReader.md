### DBS Reader server
The DBS Reader server represents DBS reader functionality, i.e.
it provides DBS APIs to access DBS back-end database. Please
refer to [DBS APIs](apis.md) for a full list of DBS APIs.
Please note, that DBS Reader server uses both GET and POST DBS APIs.

End-users can access DBS reader as simple as following:
```
# example of accessing GET API
curl https://xxx.cern.ch/dbs2go/apis

# example of accessing DataTiers DBS API, the output will be JSON data
curl https://xxx.cern.ch/dbs2go/datatiers

# example of accessing DataTiers DBS API wth ndjson output
curl -H "Accept: application/ndjson" https://xxx.cern.ch/dbs2go/datatiers

# example of accessing blockparents POST DBS API
# here client provides JSON payload via bp.json file
curl -H "Content-type: application/json" -d@$PWD/bp.json  \
    https://xxx.cern.ch/dbs2go/blockparents
```
