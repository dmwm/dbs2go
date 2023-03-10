UPDATE {{.Owner}}.DATASETS
    SET LAST_MODIFIED_BY=:myuser,
{{ if .PhysicsGroup }}
        PHYSICS_GROUP_ID = :physics_group_id,
{{ end }}
{{ if .DatasetAccessType }}
        DATASET_ACCESS_TYPE_ID = :dataset_access_type_id,
        IS_DATASET_VALID = :is_dataset_valid,
{{ end }}
        LAST_MODIFICATION_DATE=:mydate
    WHERE DATASET = :dataset
