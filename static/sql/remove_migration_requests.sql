DELETE from {{.Owner}}.MIGRATION_REQUESTS
WHERE MIGRATION_REQUEST_ID=:migration_rqst_id and create_by=:create_by
