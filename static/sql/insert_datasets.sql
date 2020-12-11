insert all
 when not exists
  (select * from {{.Owner}}.processed_datasets where processed_ds_name=processed_n)
 then
  into {{.Owner}}.processed_datasets(processed_ds_id, processed_ds_name)
  values ({{.Owner}}.seq_psds.nextval, processed_n)
 when not exists
  (select * from {{.Owner}}.dataset_access_types where dataset_access_type=access_t)
 then
  into {{.Owner}}.dataset_access_types(dataset_access_type_id, dataset_access_type)
  values ({{.Owner}}.seq_dtp.nextval, access_t)
 when exists
  (select data_tier_id from {{.Owner}}.data_tiers where data_tier_name=tier)
 then
  into {{.Owner}}.datasets
  ( dataset_id, dataset, primary_ds_id, processed_ds_id, data_tier_id,
    dataset_access_type_id, acquisition_era_id,  processing_era_id,
    physics_group_id,  xtcrosssection, prep_id, creation_date, create_by,
    last_modification_date, last_modified_by
  )
values ( {{.DatasetId}}, {{.Dataset}}, {{.PrimaryDSId}},
  nvl((select processed_ds_id
        from {{.Owner}}.sprocessed_datasets where processed_ds_name=processed_n),
       {{.Owner}}.seq_psds.nextval),
 (select data_tier_id
    from {{.Owner}}.sdata_tiers where data_tier_name=tier),
  nvl((select dataset_access_type_id
        from {{.Owner}}.dataset_access_types where dataset_access_type=access_t),
    {{.Owner}}.seq_dtp.nextval),
  {{.AcquisitionEraId}}, {{.ProcessingEraId}}, {{.PhysicsGroupId}},
  {{.Xtcrosssection}}, {{.PrepId}}, cdate, cby,
  {{.LastModificationDate}}, {{.LastModifiedBy}}
   )
   select  {{.ProcessedDSName}} processed_n,
           {{.DataTierName}} tier,  {{.DatasetAccessType}} access_t,
           {{.CreationDate}} cdate, {{.CreateBy}} cby
   from dual
