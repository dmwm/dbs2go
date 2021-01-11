SELECT DISTINCT FL.RUN_NUM FROM {{.Owner}}.FILE_LUMIS FL
{{if .Lfn}}
inner join {{.Owner}}.FILES FILES on FILES.FILE_ID = FL.FILE_ID
{{end}}
{{if .Block}}
inner join {{.Owner}}.FILES FILES on FILES.FILE_ID = FL.FILE_ID
inner join {{.Owner}}.BLOCKS BLOCKS on BLOCKS.BLOCK_ID = FILES.BLOCK_ID
{{end}}
{{if .Dataset}}
inner join {{.Owner}}.FILES FILES on FILES.FILE_ID = FL.FILE_ID
inner join {{.Owner}}.DATASETS DATASETS on DATASETS.DATASET_ID = FILES.DATASET_ID
{{end}}
