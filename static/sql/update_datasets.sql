UPDATE {{.Owner}}.DATASETS
    SET LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE=:mydate
{{ if .PhysicsGroup }}
        ,PHYSICS_GROUP_ID = :physics_group_id
{{ end }}
{{ if .DatasetAccessType }}
        ,DATASET_ACCESS_TYPE_ID = :dataset_access_type_id
        ,IS_DATASET_VALID = :is_dataset_valid
{{ end }}
    WHERE DATASET = :dataset
