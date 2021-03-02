INSERT ALL
WHEN NOT EXISTS
    (SELECT * FROM {{.Owner}}.processed_datasets
        WHERE processed_ds_name=processed_n)
THEN
    INTO {{.Owner}}.processed_datasets
    (processed_ds_id, processed_ds_name)
    VALUES ({{.Owner}}.seq_psds.nextval, processed_n)
WHEN NOT EXISTS
    w(SELECT * FROM {{.Owner}}.dataset_access_types
        WHERE dataset_access_type=access_t)
THEN
    INTO {{.Owner}}.dataset_access_types
    (dataset_access_type_id, dataset_access_type)
    VALUES ({{.Owner}}.seq_dtp.nextval, access_t)
WHEN EXISTS
    (SELECT data_tier_id FROM {{.Owner}}.data_tiers
        WHERE data_tier_name=tier)
THEN
    INTO {{.Owner}}.datasets
    ( dataset_id, dataset, primary_ds_id, processed_ds_id, data_tier_id,
      dataset_access_type_id, acquisition_era_id,  processing_era_id,
      physics_group_id,  xtcrosssection, prep_id, creation_date, create_by,
      last_modification_date, last_modified_by
    )
    VALUES ( {{.DatasetId}}, {{.Dataset}}, {{.PrimaryDSId}},
    nvl(
        (SELECT processed_ds_id
        FROM {{.Owner}}.sprocessed_datasets
        WHERE processed_ds_name=processed_n),
       {{.Owner}}.seq_psds.nextval
    ),
    (SELECT data_tier_id
    FROM {{.Owner}}.sdata_tiers
    WHERE data_tier_name=tier),
    nvl(
        (SELECT dataset_access_type_id
        FROM {{.Owner}}.dataset_access_types
        WHERE dataset_access_type=access_t),
    {{.Owner}}.seq_dtp.nextval
    ),
    :acquisition_era_id, :processing_era_id, :physics_group_id,
    :xtcrosssection, :prep_id, cdate, cby,
    :last_modification_date, :last_modified_by
    )
    SELECT  :processed_ds_name processed_n,
            :data_tier_name tier,  :dataset_access_type access_t,
            :creation_date cdate, :create_by cby
    FROM dual
