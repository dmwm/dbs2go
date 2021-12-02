package utils

import (
	"fmt"
	"net/http"
)

// DevNullWriter provides the same functionality as http.ResponseWriter
// to cover unit tests of DBS APIs. It prints given data directly to stdout.
type DevNullWriter string

// Header implements Header() API of http.ResponseWriter interface
func (s DevNullWriter) Header() http.Header {
	return http.Header{}
}

// Write implements Write API of http.ResponseWriter interface
func (s DevNullWriter) Write(b []byte) (int, error) {
	v := string(b)
	if VERBOSE > 2 {
		fmt.Println("/dev/null: ", v)
	}
	return len(v), nil
}

// WriteHeader implements WriteHeader API of http.ResponseWriter interface
func (s DevNullWriter) WriteHeader(statusCode int) {
	if VERBOSE > 2 {
		fmt.Println("/dev/null statusCode", statusCode)
	}
}
