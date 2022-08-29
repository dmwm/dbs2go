SELECT count(*) AS count FROM {{.Owner}}.MIGRATION_REQUESTS
WHERE MIGRATION_REQUEST_ID=:migration_rqst_id
      and (migration_status=0 or migration_status=3 or migration_status=9 or migration_status=5)
