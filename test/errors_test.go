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
	if !strings.Contains(err4.Error(), "Error: nil") {
		t.Error("error does not contain nil error")
	}
	fmt.Println("Wrapped error:", err4.Error())
}
