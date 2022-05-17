package main

// this file contains logic for physicsgroups API
// the HTTP request body is defined by dbs.PhysicsGroups struct defined in dbs/physicsgroups.go
// the HTTP response body is defined by physicsGroupsResponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// expected response from physicsgroups GET API
type physicsGroupsResponse struct {
	PHYSICS_GROUP_NAME string `json:"physics_group_name"`
}

// contains tests for the physicsgroups endpoint
func getPhysicsGroupsTestTable(t *testing.T) EndpointTestCase {
	physGroupReq := dbs.PhysicsGroups{
		PHYSICS_GROUP_NAME: "Tracker",
	}
	physGroupResp := physicsGroupsResponse{
		PHYSICS_GROUP_NAME: "Tracker",
	}

	pgInvalidParam := dbs.CreateInvalidParamError("fnal", "physicsgroups")
	hrec := createHTTPError("GET", "/dbs/physicsgroups?fnal=cern")
	errorResp := createServerErrorResponse(hrec, pgInvalidParam)

	return EndpointTestCase{
		description:     "Test physicsgroups",
		defaultHandler:  web.PhysicsGroupsHandler,
		defaultEndpoint: "/dbs/physicsgroups",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				serverType:  "DBSReader",
				method:      "GET",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       physGroupReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				serverType:  "DBSReader",
				method:      "GET",
				output: []Response{
					physGroupResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with invalid parameter key",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"fnal": []string{"cern"},
				},
				output: []Response{
					errorResp,
				},
				respCode: http.StatusBadRequest,
			},
		},
	}
}
