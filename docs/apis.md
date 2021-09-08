### DBS APIs
Here we list all available DBS server APIs. Please note,
the DBS servers work with either [JSON](https://www.json.org/json-en.html)
or [ndJSON](http://ndjson.org/). The latter data-format is more
suitable for data-streaming (as it does not require open/close
list brackets and commas across JSON records).

#### GET APIs
DBS GET APIS provide infomration about DBS entities.
You can use them as following:
```
# get list of datasets
# the Accept HTTP header request response in JSON data-format
curl -H "Accept: application/json" \ 
     https://some-host.com/dbs2go/datasets?dataset=/ZMM*/*/*
```
- `/datatiers`
  - return DBS data tiers
  - arguments: `data_tier_name`
- `/datasets`
  - returns list of DBS datasets, including their details
  - arguments: `dataset`, `parent_dataset`, `release_version`, `pset_hash`, `app_name,
    output_module_label`, `global_tag`, `processing_version`, `acquisition_era_name,
    run_num`, `physics_group_name`, `logical_file_name`, `primary_ds_name,
    primary_ds_type`, `processed_ds_name`, `data_tier_name`, `dataset_access_type,
    prep_id`, `create_by`, `last_modified_by`, `min_cdate`, `max_cdate`, `min_ldate,
    max_ldate`, `cdate`, `ldate`, `detail`, `dataset_id`
- `/blocks`
  - returns list of DBS blocks, including their details
  - arguments: `dataset`, `block_name`, `data_tier_name`, `origin_site_name,
    logical_file_name`, `run_num`, `min_cdate`, `max_cdate`, `min_ldate`, `max_ldate,
    cdate`, `ldate`, `open_for_writing`, `detail`
- `/blockTrio`
  - returns the triplets of files ids, run numbers and associative lumis
  - arguments: `block_name`
- `/files`
  - returns list of files including their details
  - arguments: `dataset`, `block_name`, `logical_file_name`, `release_version,
    pset_hash`, `app_name`, `output_module_label`, `run_num`, `origin_site_name,
    lumi_list`, `detail`, `validFileOnly`, `sumOverLumi`
- `/primarydatasets`
  - returns list of primary datasets
  - arguments: `primary_ds_name`, `primary_ds_type`
- `/parentDSTrio`
  - returns the triplets of files ids, run numbers and associative lumis
  - arguments: `dataset`
- `/acquisitioneras`
  - returns list of acquisition eras
  - arguments: `acquisition_era_name`
- `/releaseversions`
  - returns list of release versions
  - arguments: `release_version`, `dataset`, `logical_file_name`
- `/physicsgroups`
  - returns list of physics group names
  - arguments: `physics_group_name`
- `/primarydstypes`
  - returns list of primary dataset types
  - arguments: `primary_ds_type`, `dataset`
- `/datatypes`
  - returns list of data types
  - arguments: `datatype`, `dataset`
- `/processingeras`
  - returns list of processing eras
  - arguments: `processing_version`
- `/outputconfigs`
  - returns list of output configs
  - arguments: `dataset`, `logical_file_name`, `release_version`, `pset_hash,
    app_name`, `output_module_label`, `block_id`, `global_tag`
- `/datasetaccesstypes`
  - returns list of dataset access types
  - arguments: `dataset_access_type`
- `/runs`
  - returns list of runs including their details
  - arguments: `run_num`, `logical_file_name`, `block_name`, `dataset`
- `/runsummaries`
  - returns list of run summaries
  - arguments: `dataset`, `run_num`
- `/blockorigin`
  - returns origin site of the block
  - arguments: `origin_site_name`, `dataset`, `block_name`
- `/blockdump`
  - returns JSON dump of block information including parents, files, file lumi
    lists, dataset, etc.
  - arguments: `block_name`
- `/blockchildren`
  - returns list of block children
  - arguments: `block_name`
- `/blockparents`
  - returns list of block parents
  - arguments: `block_name`
- `/blocksummaries`
  - returns list of block summaries
  - arguments: `block_name`, `dataset`, `detail`
- `/filechildren`
  - returns list of file children
  - arguments: `logical_file_name`, `block_name`, `block_id`
- `/fileparents`
  - returns list of file parents
  - arguments: `logical_file_name`, `block_name`, `block_id`
- /filesummaries
  - returns list of file summaries
  - arguments: `block_name`, `dataset`, `run_num`, `validFileOnly`, `sumOverLumi`
- `/filelumis`
  - returns list of file lumis
  - arguments: `logical_file_name`, `block_name`, `run_num`, `validFileOnly`
- `/datasetchildren`
  - return list of dataset children
  - arguments: `dataset`
- `/datasetparents`
  - return list of dataset parents
  - arguments: `dataset`
- `/acquisitioneras_ci`
  - returns list of acquisition eras
  - arguments: `acquisition_era_name`

##### informative APIs provides additional information about DBS server
- `/status`
  - returns HTTP status of DBS server, can be used by liveness probe
  - arguments: None
- `/serverinfo`
  - returns server information about DBS server
  - arguments: None
- `/help`
  - returns list of DBS APIs supported by DBS server
  - arguments: None
- `/metrics`
  - return DBS server metrics suitable for Prometheus
  - arguments: None

#### POST APIs
The POST APIs are used both by DBS Reader and DBS Writer servers. In former
case, they are used to request information from DBS by providing input in JSON
data-format. In latter case, they are used to inject data into DBS Writer
server.

You can use them as following:
```
# inject data tier info into DBS server
# the Content-Type header instructs api that input is in JSON data-format
# the Accept HTTP header requests information in JSON data-format
curl -X POST -H "Content-Type: applicatin/json" -H "Accept: application/json" \
     -d@/path/datatiers.json https://some-host.com/dbs2go/datatiers
```
##### data injection APIs used by DBS Writer server
- `/datatiers`
  - injects data tier information to DBS
  - inputs:
- `/datasets`
  - injects dataset information to DBS
  - inputs:
- `/blocks`
  - injects blocks information to DBS
  - inputs:
- `/bulkblocks`
  - injects blocks information in bulk request to DBS
  - inputs:
- `/files`
  - injects file information to DBS
  - inputs:
- `/primarydatasets`
  - injects primary datasets information to DBS
  - inputs:
- `/acquisitioneras`
  - injects acquisition eras information to DBS
  - inputs:
- `/processingeras`
  - injects processing eras information to DBS
  - inputs:
- `/outputconfigs`
  - injects output configs information to DBS
  - inputs:
- `/fileparents`
  - injects file paretage information to DBS
  - inputs:

##### data look-up APIs by DBS Reader server
- `/datasetlist`
  - injects 
  - inputs:
- `/fileparentsbylumi`
  - injects 
  - inputs:
- `/fileArray`
  - injects file information to DBS
  - inputs:
- `/filelumis`
  - injects file lumis information to DBS
  - inputs:
- `/blockparents`
  - injects block parents information to DBS
  - inputs:

### PUT DBS APIs
The PUT APIs are used to update some information in DBS entities.

You can use them as following:
```
# update dataset information in DBS
# the Content-Type header instructs api that input is in JSON data-format
# the Accept HTTP header request response in JSON data-format
curl -X PUT -H "Content-Type: applicatin/json" -H "Accept: application/json" \
     -d@/path/datasets.json https://some-host.com/dbs2go/datasets
```
##### data update APIs used by DBS Writer server
- `/datasets`
  - updates dataset information to DBS
  - inputs:
- `/blocks`
  - updates blocks information to DBS
  - inputs:
- `/files`
  - updates file information to DBS
  - inputs:
- `/acquisitioneras`
  - updates acquisition eras information to DBS
  - inputs:

#### DBS Migration server APIs
The DBS Migration server has its own set of APIs. They are listed below:
- `/submit`
  - submits migration request to DBS server
  - arguments:
- `/process`
  - invoke process request
  - arguments:
- `/remove`
  - retmove given request from DBS server
  - arguments:
- `/status`
  - returns status of DBS migration requests
  - arguments: None
- `/total`
  - returns total number of migration requests in DBS
  - arguments: None
- `/serverinfo`
  - returns server information about DBS server
  - arguments: None
