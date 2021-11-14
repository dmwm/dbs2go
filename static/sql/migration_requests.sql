SELECT MR.MIGRATION_REQUEST_ID, MR.MIGRATION_URL,
       MR.MIGRATION_INPUT, MR.MIGRATION_STATUS,
       MR.CREATE_BY, MR.CREATION_DATE,
       MR.LAST_MODIFIED_BY, MR.LAST_MODIFICATION_DATE, MR.RETRY_COUNT
FROM {{.Owner}}.MIGRATION_REQUESTS MR

{{if .Oldest}}
WHERE MR.MIGRATION_STATUS=0
or (MR.migration_status=3 and MR.retry_count=0 and MR.last_modification_date <= {{.Date1}})
or (MR.migration_status=3 and MR.retry_count=1 and MR.last_modification_date <= {{.Date2}})
or (MR.migration_status=3 and MR.retry_count=2 and MR.last_modification_date <= {{.Date3}})
ORDER BY MR.creation_date
{{end}}
