SELECT
    PD.PRIMARY_DS_ID,
    PD.CREATE_BY,
    PDT.PRIMARY_DS_TYPE,
    PD.PRIMARY_DS_NAME,
    PD.CREATION_DATE
FROM {{.Owner}}.PRIMARY_DATASETS PD
JOIN {{.Owner}}.DATASETS D ON D.PRIMARY_DS_ID = PD.PRIMARY_DS_ID
JOIN {{.Owner}}.PRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = PD.PRIMARY_DS_TYPE_ID
WHERE D.DATASET = :dataset
