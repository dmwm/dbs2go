SELECT
    MIGRATION_BLOCK_ID,
    MIGRATION_BLOCK_NAME,
    MIGRATION_ORDER,
    MIGRATION_STATUS
FROM {{.Owner}}.MIGRATION_BLOCKS
WHERE MIGRATION_REQUEST_ID=:migration_request_id
