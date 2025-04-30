package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
)

func helper() error {
	err := errors.New("test")
	return dbs.Error(err, dbs.GenericErrorCode, "message", "helper")
}

var ErrorTest = errors.New("test")

func helper2() error {
	return fmt.Errorf("helper2 %w", ErrorTest)
}

// TestDBSError is based on the following examples
// https://gosamples.dev/check-error-type/
func TestDBSError(t *testing.T) {
	dbsErr := helper()
	var e *dbs.DBSError
	if errors.As(dbsErr, &e) {
		fmt.Println("dbsErr is type of DBSError")
	} else {
		t.Error("dbsErr did not trigger errors.As")
	}
	err := errors.New("test")
	if errors.As(err, &e) {
		t.Error("provided generic error is not type of DBSError")
	}

	err2 := helper2()
	if errors.Is(err2, ErrorTest) {
		fmt.Println("err is wrapper around ErrorTest")
	} else {
		t.Error("err is not wrapper around ErrorTest")
	}

	// print wrapper error
	err3 := dbs.Error(dbsErr, dbs.ParseErrorCode, "wrapper", "TestDBSError")
	if !strings.Contains(err3.Error(), "TestDBSError") {
		t.Error("error does not contain reason")
	}
	if !strings.Contains(err3.Error(), "helper") {
		t.Error("error does not contain reason for underlying helper function")
	}
	fmt.Println("Wrapped error:", err3.Error())

	err4 := dbs.Error(nil, dbs.GenericErrorCode, "nil wrapper", "TestDBSError")
	if !strings.Contains(err4.Error(), "nil") {
		t.Error("error does not contain nil error")
	}
	fmt.Println("Wrapped error:", err4.Error())
}

// TestDBSErrorIntegration test DBSError integration
func TestDBSErrorIntegration(t *testing.T) {
	dbsErr := helper()
	var e *dbs.DBSError
	if !errors.As(dbsErr, &e) {
		t.Error("dbsErr did not trigger errors.As")
	}
	err1 := dbs.Error(dbsErr, dbs.BlockAlreadyExists, "message", "func.TestDBSErrorIntegration")
	if !strings.Contains(err1.Error(), "block already exists") {
		t.Error("fail to match explanation for: block already exists")
	}
	err2 := dbs.Error(dbsErr, dbs.InsertBlockErrorCode, "message", "func.TestDBSErrorIntegration")
	if !strings.Contains(err2.Error(), "insert block error") {
		t.Error("fail to match explanation for: insert block error")
	}

	// create nested error
	err1 = dbs.Error(errors.New("lower deep level"), dbs.GetDataTierIDErrorCode, "message", "func.TestDBSErrorIntegration")
	err2 = dbs.Error(err1, dbs.InsertBlockErrorCode, "message", "func.TestDBSErrorIntegration")
	if !strings.Contains(err2.Error(), fmt.Sprintf("%d", dbs.GetDataTierIDErrorCode)) {
		t.Error("DBSError does not contain GetDataTierIDErrorCode")
	}
	if !strings.Contains(err2.Error(), fmt.Sprintf("%d", dbs.InsertBlockErrorCode)) {
		t.Error("DBSError does not contain InsertBlockErrorCode")
	}
	t.Log("example of nested error")
	t.Log(err2.Error())

}

// TestDBSErrorCodesValues test DBSError code values
func TestDBSErrorCodesValues(t *testing.T) {
	if dbs.FileDataTypesDoesNotExist != 201 { // hard-coded value in dbs/errors.go
		t.Log("Wrong code value for FileDataTypesDoesNotExist")
	}
	if dbs.InsertOutputConfigErrorCode != 317 { // hard-coded value in dbs/errors.go
		t.Log("Wrong code value for InsertOutputConfigErrorCode")
	}
}
