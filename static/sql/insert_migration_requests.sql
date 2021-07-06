INSERT INTO {{.Owner}}.MIGRATION_REQUESTS
(MIGRATION_REQUEST_ID, MIGRATION_URL, MIGRATION_INPUT, MIGRATION_STATUS, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY)
VALUES(:migration_request_id, :migration_url, :migration_input, :migration_status, :creation_date, :create_by, :last_modification_date, :last_modified_by)
