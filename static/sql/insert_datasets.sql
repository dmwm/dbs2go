INSERT INTO {{.Owner}}.DATASETS
    (dataset_id,dataset,is_dataset_valid,primary_ds_id,
     processed_ds_id,data_tier_id,dataset_access_type_id,
     acquisition_era_id,processing_era_id,physics_group_id,
     xtcrosssection,prep_id,creation_date,create_by,
     last_modification_date,last_modified_by)
    VALUES
    (:dataset_id,:dataset,:is_dataset_valid,:primary_ds_id,
     :processed_ds_id,:data_tier_id,:dataset_access_type_id,
     :acquisition_era_id,:processing_era_id,:physics_group_id,
     :xtcrosssection,:prep_id,:creation_date,:create_by,
     :last_modification_date,:last_modified_by)
