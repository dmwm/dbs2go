INSERT INTO {{.Owner}}.MIGRATION_BLOCKS
    (MIGRATION_BLOCK_ID, MIGRATION_REQUEST_ID, MIGRATION_BLOCK_NAME, MIGRATION_ORDER, MIGRATION_STATUS, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY)
VALUES (:migration_block_id, :migration_request_id, :migration_block_name, :migration_order, :migration_status, :creation_date, :create_by, :last_modification_date, :last_modified_by)
