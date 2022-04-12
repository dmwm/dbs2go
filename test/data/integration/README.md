# Integration Test Data
This folder contains data for integration tests. The main file, `integration_data.json`, contains most of the metadata that is used in all of the integration tests (`test/int_*.go`).

The data is generated in `test/integration_cases.go`. The JSON structure is defined in the struct `initialData` in the same file. When running `make test-integration`, the function `TestIntegration` in `test/integration_test.go` is run, which does the following:
1. Load the data from the file defined in `INTEGRATION_DATA_FILE`. The default in the `MakeFile` is `test/data/integration_data.json`. The data is loaded into the variable `TestData` in `test/integration_cases.go`.
2. Populate the test cases with this initial data.
3. Iterate over the testCases and run the data through `runTestWorkflow`.
4. Each testCase either does a `POST` or `GET` request, depending on the fields in the testCase structure.

When running `make test-integration`, if the file at `INTEGRATION_DATA_FILE` does not exist, the function `generateBaseData` in `test/integration_cases.go` will be run, populating the fields in `TestData` and then writing the data as JSON into the file.

## Writing Test Cases
The test cases are written as Table-Driven tests, in which only the inputs and expected outputs for each case are needed in a struct list.

Each endpoint of the API has a corresponding `test/int_*.go`.
The test cases related to the endpoint are created in `get*TestTable` functions in each file, which returns a slice of `EndpointTestCase`. 
`EndpointTestCase` struct contains default information about an endpoint.

Within `EndpointTestCase`, the field `testCases` contains a list of test cases that utilize the endpoint.
An individual test case is defined in the `testCase` struct in `test/integration_cases.go`. It defines the basic elements to create a test case.

## Processing Test Cases
Each `EndpointTestCase` is run through `runTestWorkflow` in `test/integration_test.go`. In turn, the `testCases` field is iterated over, using the `testCase` fields in individual test cases.