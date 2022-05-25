{{.TokenGenerator}}
{{if .SQLite}}
UPDATE {{.Owner}}.FILES
{{else}}
UPDATE {{.Owner}}.FILES F
{{end}}
    SET LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE=:mydate,
        IS_FILE_VALID = :is_file_valid
{{if .Dataset}}
{{if .SQLite}}
AND dataset_id in (
{{else}}
AND F.dataset_id in (
{{end}}
    SELECT D.dataset_id FROM {{.Owner}}.DATASETS D
    INNER JOIN {{.Owner}}.FILES F2 ON F2.dataset_id = D.dataset_id
    WHERE D.dataset=:dataset
)
{{end}}
