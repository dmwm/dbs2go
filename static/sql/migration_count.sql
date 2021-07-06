select count(*) as count from {{.Owner}}.MIGRATION_REQUESTS
WHERE MIGRATION_REQUEST_ID=:migration_rqst_id and create_by=:create_by
and (migration_status=0 or migration_status=3 or migration_status=9)
