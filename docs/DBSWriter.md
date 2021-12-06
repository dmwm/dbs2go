### DBS Writer server
The DBS Writer server represents DBS writer functionality, i.e.
it provides DBS APIs to write to DBS back-end database. Please
refer to [DBS APIs](apis.md) for a full list of DBS APIs.
Please note, that DBS Writer server uses both POST and PUT DBS APIs.

End-users can access DBS writer as simple as following:
```
# example of accessing bulkblocks POST DBS API
# here client provides JSON payload via b.json file
curl -H "Content-type: application/json" \
    -d@$PWD/bp.json \
    https://xxx.cern.ch/dbs2go/bulkblocks

# example of accessing bulkblocks POST DBS API
# usng gzip'ed json payload file b.json.gz
curl -H "Content-type: application/json" \
    -H "Content-Encoding: gzip" --data-binary @$PWD/b.json.gz \
    https://xxx.cern.ch/dbs2go/bulkblocks
```
