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
- `/apis`
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
  - inputs, for exact defition see [DataTiers](../dbs/tiers.go) struct, e.g.
```
{
    "data_tier_id": 123,
    "data_tier_name": "RAW",
    "creation_date": 1631118749,
    "create_by": "tester"
}
```
- `/datasets`
  - injects dataset information to DBS
  - inputs, for exact defition see [DatasetRecord](../dbs/datasets.go) struct, e.g.
```
{
    "dataset": "/a/c/RAW",
    "primary_ds_name": "primary",
    "processed_ds": "processed_dataset",
    "data_tier": "RAW",
    "acquisition_era": "era",
    "dataset_access_type": "access-type",
    "processing_version": "version",
    "physics_group": "group",
    "xtcrosssection": 1.2,
    "creation_date": 1631118749,
    "create_by": "tester",
    "last_modification_date": 1631118749,
    "last_modified_by": "tester"
}
```
- `/blocks`
  - injects blocks information to DBS
  - inputs, for exact defition see [BlockRecord](../dbs/blocks.go) struct, e.g.
```
{
    "block_name": "/a/b/RAW#123",
    "open_for_writing": 1,
    "origin_site_name": "site",
    "block_size": 123,
    "file_count": 111,
    "creation_date": 1631118749,
    "create_by": "tester",
    "last_modification_date": 1631118749,
    "last_modified_by": "tester
}
```
- `/bulkblocks`
  - injects blocks information in bulk request to DBS
  - inputs, for exact defition see [BulkBlocks](../dbs/bulkblocks.go) struct, e.g.
```
{
  "dataset_conf_list": [
    {
      "release_version": "CMSSW_1_2_3",
      "pset_hash": "76e303993a1c2f842159dbfeeed9a0dd",
      "app_name": "cmsRun",
      "output_module_label": "Merged",
      "global_tag": "my-cms-gtag::ALL"
    }
  ],
  "file_conf_list": [
    {
      "release_version": "CMSSW_1_2_3",
      "pset_hash": "76e303993a1c2f842159dbfeeed9a0dd",
      "lfn": "/store/data/a/b/A/a/1/abcd0.root",
      "app_name": "cmsRun",
      "output_module_label": "Merged",
      "global_tag": "my-cms-gtag::ALL"
    }
  ],
  "files": [
    {
      "file_lumi_list": [
        {
          "lumi_section_num": 27414,
          "run_num": 1
        },
        {
          "lumi_section_num": 26422,
          "run_num": 2
        },
        {
          "lumi_section_num": 29838,
          "run_num": 3
        }
      ],
      "event_count": 1619,
      "file_type": "EDM",
      "last_modified_by": "Yuyi",
      "logical_file_name": "/store/data/a/b/A/a/1/abcd9.root",
      "file_size": 2012211901,
      "last_modification_date": 1279912089,
      "auto_cross_section": 0
    }
  ],
  "processing_era": {
    "create_by": "Yuyi",
    "processing_version": 10,
    "description": "this_is_a_test"
  },
  "primds": {
    "create_by": "Yuyi",
    "primary_ds_type": "test",
    "primary_ds_name": "unittest_web_primary_ds_name_14144",
    "creation_date": 1279912078
  },
  "dataset": {
    "physics_group_name": "Tracker",
    "create_by": "Yuyi",
    "dataset_access_type": "PRODUCTION",
    "data_tier_name": "GEN-SIM-RAW",
    "last_modified_by": "Yuyi",
    "creation_date": 1279912078,
    "processed_ds_name": "Summer2011-pstr-v10",
    "xtcrosssection": 123,
    "last_modification_date": 1279912078,
    "dataset": "/unittest_web_primary_ds_name_14144/Summer2011-pstr-v10/GEN-SIM-RAW"
  },
  "acquisition_era": {
    "acquisition_era_name": "Summer2011",
    "start_date": 1978
  },
  "block": {
    "create_by": "Yuyi",
    "creation_date": 1279912079,
    "open_for_writing": 1,
    "block_name": "/unittest_web_primary_ds_name_14144/Summer2011-pstr-v10/GEN-SIM-RAW#141444",
    "file_count": 10,
    "origin_site_name": "my_site",
    "block_size": 20122119010
  },
  "file_parent_list": [
    {
      "logical_file_name": "/store/data/a/b/A/a/1/abcd4.root",
      "parent_logical_file_name": "/store/data/a/b/A/a/1/abcd3.root_15825"
    }
  ]
}
```
- `/files`
  - injects file information to DBS
  - inputs, for exact defition see [FileRecord](../dbs/files.go) struct, e.g.
```
{
    "logical_file_name": "/path/lfn.root",
    "is_file_valid": 1,
    "dataset": "/a/b/RAW",
    "block": "a/b/RAW#123",
    "file_type": "EDM",
    "check_sum": "1123ljsdkfjsd",
    "file_size": 123,
    "event_count": 111,
    "adler32", "adler",
    "md5": "md5",
    "auto_cross_section": 1.1,
    "createion_date": 1631118749,
    "create_by": "tester",
    "last_modification_date": 1631118749,
    "last_modified_by": "tester",
    "file_lumi_list": [
        {
          "lumi_section_num": 27414,
          "run_num": 1
        }
    ],
    "file_parent_list": [
        {
          "logical_file_name": "/store/data/a/b/A/a/1/abcd4.root",
          "parent_logical_file_name": "/store/data/a/b/A/a/1/abcd3.root_15825"
        }
    ],
    "file_output_config_list": [
        {
          "app_name": "application",
          "release_version": "version",
          "pset_hash": "hash",
          "pset_name": "pset",
          "global_tag": "global_tag",
          "output_module_label": "label",
          "creation_date": 1631118749,
          "create_by": "tester",
          "scenario": "scenario"
        }
    ]
}
```
- `/primarydatasets`
  - injects primary datasets information to DBS
  - inputs, for exact defition see [PrimaryDatasetRecord](../dbs/primarydatasets.go) struct, e.g.
```
{
    "primary_ds_name": "primary",
    "primary_ds_type": "primary-type",
    "createion_date": 1631118749,
    "create_by": "tester"
}
```
- `/acquisitioneras`
  - injects acquisition eras information to DBS
  - inputs, for exact defition see [AcquisitionEras](../dbs/acquisitioneras.go) struct, e.g.
```
{
    "acquisition_era_id": 123,
    "acquisition_era_name": "era",
    "start_date": 1631118749,
    "end_date": 1631118749,
    "create_date": 1631118749,
    "create_by": "tester",
    "description": "note"
}
```
- `/processingeras`
  - injects processing eras information to DBS
  - inputs, for exact defition see [ProcessingEras](../dbs/processingeras.go) struct, e.g.
```
{
    "processing_era_id": 123,
    "processing_version": 12345,
    "creation_date": 1631118749,
    "create_by": "tester",
    "description": "note"
}
```
- `/outputconfigs`
  - injects output configs information to DBS
  - inputs, for exact defition see [OutputConfigRecord](../dbs/outputconfigs.go) struct, e.g.
```
{
    "app_name": "application",
    "release_version": "release",
    "pset_hash": "hash",
    "pset_name": "name",
    "global_tag": "tag",
    "output_module_label": "label",
    "creation_date": 1631118749,
    "create_by": "tester",
    "scenario": "note"
}
```
- `/fileparents`
  - injects file paretage information to DBS
  - inputs, for exact defition see [FileParentRecord](../dbs/fileparents.go) struct, e.g.
```
{
    "logical_file_name": "/a/g/file.root",
    "parent_logical_file_name": "/a/b/file.root"
}
```

##### data look-up APIs by DBS Reader server
- `/datasetlist`
  - provides list of dataset for given JSON record 
  - inputs:
- `/fileparentsbylumi`
  - provides file parents for given set of lumis
  - inputs:
- `/fileArray`
  - provides list of file and their details for given JSON record
  - inputs: JSON record containing the following parameters:
  `dataset`, `block_name`, `lumi_list`, `run_num`, `detail`, `validFileOnly`,
  `sumOverLumi`, e.g
```
{
    "block_name": "/a/b/GEN-SIM-RAW#52787",
    "lumi_list": [1, 2, 3, 4, 5, 6],
    "run_num": 97,
    "detail": 1
}
```
- `/filelumis`
  - provides list of file lumis for given JSON record
  - inputs: JSON record containing the following parameters:
  `logical_file_name`, `block_name`, `run_num`, `validFileOnly`, e.g.
```
{
    "logical_file_name": ["/path/file.root", /path2/file.root"],
    "run_num": [97,98],
    'validFileOnly": 0
}
```
- `/blockparents`
  - provides block parents for given JSON record
  - inputs: JSON record with possible list of `block_name` values, e.g.
```
{
    "block_name": ["/a/b/RAW#123", "/a/b/RAW@234"]
}
```

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
