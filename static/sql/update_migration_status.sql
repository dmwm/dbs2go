UPDATE {{.Owner}}.MIGRATION_REQUESTS
    SET MIGRATION_STATUS = :status, RETRY_COUNT = :retry_count
    WHERE MIGRATION_REQUEST_ID = :migration_request_id
