SELECT
    PE.CREATE_BY,
    PE.PROCESSING_VERSION,
    PE.DESCRIPTION
FROM {{.Owner}}.PROCESSING_ERAS PE
JOIN {{.Owner}}.DATASETS D ON D.PROCESSING_ERA_ID = PE.PROCESSING_ERA_ID
WHERE D.DATASET = :dataset
