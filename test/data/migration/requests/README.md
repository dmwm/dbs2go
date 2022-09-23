# Migration Requests
* Place migration requests as JSON format in this directory.
* Run `db_snapshot` to start the DBSMigrate and DBSMigration servers
    * Currently commented are the scripts to create a snapshot of datasets into a local database.
* Run `make test-migration-requests` afterwards to test migration requests in this directory.