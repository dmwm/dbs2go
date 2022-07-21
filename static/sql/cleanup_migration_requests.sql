DELETE FROM {{.Owner}}.MIGRATION_REQUESTS
WHERE LAST_MODIFICATION_DATE <= {{.Value}}
or (migration_status=3 and retry_count=3 and last_modification_date <= {{.FailDate}})
or (migration_status=9 and retry_count=3 and last_modification_date <= {{.FailDate}})
