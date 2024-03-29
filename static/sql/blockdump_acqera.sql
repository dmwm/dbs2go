SELECT
    AE.ACQUISITION_ERA_NAME,
    AE.START_DATE,
    AE.CREATION_DATE,
    AE.CREATE_BY,
    AE.DESCRIPTION
FROM {{.Owner}}.ACQUISITION_ERAS AE
JOIN {{.Owner}}.DATASETS D ON D.ACQUISITION_ERA_ID = AE.ACQUISITION_ERA_ID
WHERE D.DATASET = :dataset
