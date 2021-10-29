UPDATE {{.Owner}}.DATASETS
    SET LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE=:mydate,
        DATASET_ACCESS_TYPE = :dataset_access_type,
        IS_DATASET_VALID = :is_dataset_valid
    WHERE DATASET = :dataset
