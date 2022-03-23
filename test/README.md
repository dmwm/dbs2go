This is a test area for DBS unit tests. It includes the following files:
- `data` foler contains data used by unit tests, e.g.
  - `data/insert.yaml` represents list of insert/look-up APIs to be included with
  TestInsertSQL unit tests (see `sql_test.go` file)
  - `data/primarydataset.json` represent data record used by injection
  DBS API to insert primary dataset
- `sql.yaml` contains list of APIs and parameters for look-up SQL tests, see
`sql_test.go` TestSQL api
- `xxx_test.go` represent individual test files for different DBS APIs

## Integration Testing
Running `make integration` will run the integration tests contained in the following files:
- `integration_test.go` contains the main test workflow for integration tests
- `integration_cases.go` contains the logic for implementing the table-driven integration tests
- `int_*.go` represents files that contains the tables for integration tests
- `data/integration/integration_data.json` is the initial data to be populated into the test case tables

### Requirements
The following environment variables are required:
- `PKG_CONFIG_PATH`: This is the location of the `oci8.pc` file
- `DYLD_LIBRARY_PATH`: This is the location of the Oracle instantclient files
  - The instructions to prepare the files and directories for these are in the [Installation instructions](docs/Installation.md)
- `DBS_READER_LEXICON_FILE`: Lexicon file for DBSReader server; default: `static/lexicon_reader.json`
- `DBS_WRITER_LEXICON_FILE`: Lexicon file for DBSWriter server; default: `static/lexicon_writer.json`
- `INTEGRATION_DATA_FILE`: File for initial data for test case tables; default: `test/data/integration/integration_data.json`