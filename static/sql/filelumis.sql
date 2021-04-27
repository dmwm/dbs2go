{{.TokenGenerator}}

SELECT DISTINCT FL.RUN_NUM as RUN_NUM, FL.LUMI_SECTION_NUM as LUMI_SECTION_NUM, FL.EVENT_COUNT as EVENT_COUNT

{{if .Lfn}} {{/* Lfn block */}}

{{if .LfnList}} {{/* LfnList block */}}

{{if .ValidFileOnly}} {{/* validFileOnly block */}}

, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL
JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID
JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID

{{else}}

{{if .Migration}} {{/* migration block */}}
FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID
{{else}}
, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID
{{end}} {{/* enf of migration block */}}

{{end}} {{/* end of validFileOnly block */}}

{{else}} {{/* else LfnList block */}}

, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID

{{if .ValidFileOnly}} {{/* validFileOnly block */}}
JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
{{else}}

{{end}} {{/* end of validFileOnly block */}}

{{end}} {{/* end of LfnList block */}}

{{else if .BlockName}} {{/* else in Lfn block */}}

, F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM {{.Owner}}.FILE_LUMIS FL JOIN {{.Owner}}.FILES F ON F.FILE_ID = FL.FILE_ID

{{if .ValidFileOnly}} {{/* validFileOnly block */}}
JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
JOIN {{.Owner}}.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
{{else}}
JOIN {{.Owner}}.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
{{end}} {{/* end of validFileOnly block */}}

{{end}} {{/* end of Lfn block */}}
