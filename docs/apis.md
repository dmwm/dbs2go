### DBS APIs
Here we list all available client RESTful APIs for DBS server:

#### GET APIs
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

#### POST APIs 
- /bulkblocks
  - allows to inject block information about DBS blocks
  - inputs:
- /fileArray
- /datasetlist
- /fileparentsbylumi
- /datatiers
- /datasets
- /blocks
- /bulkblocks
- /files
- /primarydatasets
- /acquisitioneras
- /processingeras
- /outputconfigs
- /blockparents
- /fileparents
- /filelumis

#### Server aux APIs
- /status
- /serverinfo
- /help
- /metrics

#### DBS Migration server APIs
- /submit
- /process
- /remove
- /status
- /total
- /serverinfo
