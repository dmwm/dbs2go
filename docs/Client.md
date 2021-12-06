### DBS client
The `dbs2go` is a fully featured HTTP service. Therefore, it 
allows access from any client via HTTP requests. Here we
outline access via `curl` tool:

```
# perform GET request to request some data from DBS server
# below we'll use https://xxx.cern.ch/dbs2go as an example of
# DBS server
curl -L -k --key ~/.globus/userkey.pem --cert ~/.globus/usercert.pem \
    -H "Accept: application/json" \
     https://xxx.cern.ch/dbs2go/datasets?dataset=/ZMM*/*/*

# example of POST API, to upload upload.json file to fileArray API
curl -L -k --key ~/.globus/userkey.pem --cert ~/.globus/usercert.pem \
    -H "Content-type: application/json" -H "Accept: application/json" \
    -d@/tmp/upload.json \
     https://xxx.cern.ch/dbs2go/fileArray

# example of POST API, to upload b.json.gz file to bulkblocks API
curl -L -k --key ~/.globus/userkey.pem --cert ~/.globus/usercert.pem \
    -H "Content-type: application/json" -H "Accept: application/json" \
    -H "Content-Encoding: gzip" --data-binary @$PWD/b.json.gz \
     https://xxx.cern.ch/dbs2go/bulkblocks

```
