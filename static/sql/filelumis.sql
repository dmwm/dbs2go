{{.TokenGenerator}}

SELECT DISTINCT FL.RUN_NUM as RUN_NUM, FL.LUMI_SECTION_NUM as LUMI_SECTION_NUM, FL.EVENT_COUNT as EVENT_COUNT

{{if .Lfn}} {{/* Lfn block */}}

{{if .LfnList}} {{/* LfnList block */}}

{{if .ValidFileOnly eq 0}} {{/* validFileOnly block */}}

{{if .Migration}} {{/* migration block */}}
FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID WHERE F.LOGICAL_FILE_NAME = :logical_file_name
{{else}}
, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID WHERE F.LOGICAL_FILE_NAME = :logical_file_name
{{end}} {{/* enf of migration block */}}

{{else}}

, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL 
JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID
JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
WHERE F.IS_FILE_VALID = 1 AND F.LOGICAL_FILE_NAME = :logical_file_name
AND DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')

{{end}} {{/* end of validFileOnly block */}}

{{else if .LfnList}}

, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID

{{if .ValidFileOnly eq 0}} {{/* validFileOnly block */}}
WHERE F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)
{{else}}
JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
WHERE F.IS_FILE_VALID = 1 AND F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)
AND DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')
{{end}} {{/* end of validFileOnly block */}}

{{end}} {{/* end of LfnList block */}}

{{else if .BlockName}} {{/* else in Lfn block */}}

, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}. FILES F ON F.FILE_ID = FL.FILE_ID 

{{if .ValidFileOnly eq 0}} {{/* validFileOnly block */}}
JOIN {{.Owner}}.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
WHERE B.BLOCK_NAME = :block_name
{{else}}
JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
JOIN {{.Owner}}.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
WHERE F.IS_FILE_VALID = 1 AND B.BLOCK_NAME = :block_name
AND DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')
{{end}} {{/* end of validFileOnly block */}}

{{end}} {{/* end of Lfn block */}}
