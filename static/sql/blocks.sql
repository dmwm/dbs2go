{{.TokenGenerator}}
{{if .Runs}}
SELECT DISTINCT
{{else}}
SELECT
{{end}}
{{if .Detail}}
    B.BLOCK_ID, B.BLOCK_NAME, B.OPEN_FOR_WRITING, 
    B.BLOCK_SIZE, B.FILE_COUNT,
    B.DATASET_ID, DS.DATASET,
    B.ORIGIN_SITE_NAME, B.CREATION_DATE, B.CREATE_BY,
    B.LAST_MODIFICATION_DATE, B.LAST_MODIFIED_BY
{{else}}
    B.BLOCK_NAME
{{end}}
{{if .Runs}}
    , FLM.RUN_NUM
{{end}}
FROM {{.Owner}}.BLOCKS B JOIN {{.Owner}}.DATASETS DS ON DS.DATASET_ID = B.DATASET_ID 
{{if .Lfns}}
JOIN {{.Owner}}.FILES FL ON FL.BLOCK_ID = B.BLOCK_ID
{{else}}
{{if .Runs}}
JOIN {{.Owner}}.FILES FL ON FL.BLOCK_ID = B.BLOCK_ID
{{end}}
{{end}}
{{if .Runs}}
JOIN {{.Owner}}.FILE_LUMIS FLM on FLM.FILE_ID = FL.FILE_ID
{{end}}
