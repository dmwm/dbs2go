UPDATE {{.Owner}}.DATASETS
    SET LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE=:mydate,
        DATASET_ACCESS_TYPE_ID = :dataset_access_type_id,
        IS_DATASET_VALID = :is_dataset_valid
    WHERE DATASET = :dataset
