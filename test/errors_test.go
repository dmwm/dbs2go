package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
)

func helper() error {
	err := errors.New("test")
	return dbs.Error(err, dbs.GenericError, "", "helper")
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
}
