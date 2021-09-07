### DBS client
The `dbs2go` is a full featured RESTful HTTP service. Therefore, it follows
HTTP standard and allow access from any client via HTTP requests. Here we
outline access via `curl` tool:

```
# perform GET request to request some data from DBS server
# below we'll use https://xxx.cern.ch/dbs2go as an example of
# DBS server
curl -L -k --key ~/.globus/userkey.pem --cert ~/.globus/usercert.pem \
     https://xxx.cern.ch/dbs2go/datasets?dataset=/ZMM*/*/*

# example of POST API, to upload upload.json file to fileArray API
curl -L -k --key ~/.globus/userkey.pem --cert ~/.globus/usercert.pem \
    -d@/tmp/upload.json \
     https://xxx.cern.ch/dbs2go/fileArray

```
