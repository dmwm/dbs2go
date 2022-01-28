package dbs

import (
	"fmt"
)

// DBS Error codes provides static representation of DBS errors
const (
	GenericError = iota
	DatabaseError
	TransactionError
	QueryError
	RowsScanError
	SessionError
	CommitError
	ParseError
	LoadError
	GetIDError
	InsertError
	LastInsertError
	ValidateError
	PatternError
	DecodeError
	EncodeError
	ContentTypeError
	ParametersError
	NotImplementedApi
	ReaderError
)

// DBSError represents common structure for DBS errors
type DBSError struct {
	Reason   string `json:"reason"`   // error string
	Message  string `json:"message"`  // additional message describing the issue
	Code     int    `json:"code"`     // DBS error code
	Function string `json:"function"` // DBS function
}

// Error function implements details of DBS error message
func (e *DBSError) Error() string {
	return fmt.Sprintf("code=%d function=%s msg=%s error=%v", e.Code, e.Function, e.Message, e.Reason)
}

// helper function to create dbs error
func Error(err error, code int, msg, function string) error {
	return &DBSError{
		Reason:   err.Error(),
		Message:  msg,
		Code:     code,
		Function: function,
	}
}
