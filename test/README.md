This is a test area for DBS unit tests. It includes the following files:
- `data` foler contains data used by unit tests, e.g.
  - `data/insert.yaml` represents list of insert/look-up APIs to be included with
  TestInsertSQL unit tests (see `sql_test.go` file)
  - `data/primarydataset.json` represent data record used by injection
  DBS API to insert primary dataset
- `sql.yaml` contains list of APIs and parameters for look-up SQL tests, see
`sql_test.go` TestSQL api
- `xxx_test.go` represent individual test files for different DBS APIs
