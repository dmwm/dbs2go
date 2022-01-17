DELETE FROM {{.Owner}}.MIGRATION_REQUESTS MR
WHERE MR.LAST_MODIFICATION_DATE <= {{.Value}}
or (MR.migration_status=3 and MR.retry_count=3 and MR.last_modification_date <= {{.FailDate}})
or (MR.migration_status=9 and MR.retry_count=3 and MR.last_modification_date <= {{.FailDate}})
