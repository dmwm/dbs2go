package dbs

import (
	"errors"
	"fmt"
)

// GenericErr represents generic dbs error
var GenericErr = errors.New("dbs error")

// DatabaseErr represents generic database error
var DatabaseErr = errors.New("database error")

// InvalidParamErr represents generic error for invalid input parameter
var InvalidParamErr = errors.New("invalid parameter(s)")

// ConcurrencyErr represents generic concurrency error
var ConcurrencyErr = errors.New("concurrency error")

// RecordErr represents generic record error
var RecordErr = errors.New("record error")

// ValidationErr represents generic validation error
var ValidationErr = errors.New("validation error")

// ContentTypeErr represents generic content-type error
var ContentTypeErr = errors.New("content-type error")

// NotImplementedApiErr represents generic not implemented api error
var NotImplementedApiErr = errors.New("not implemented api error")

// InvalidRequestErr represents generic invalid request error
var InvalidRequestErr = errors.New("invalid request error")

// DBS Error codes provides static representation of DBS errors, they cover 1xx range
const (
	GenericErrorCode        = iota + 100 // generic DBS error
	DatabaseErrorCode                    // 101 database error
	TransactionErrorCode                 // 102 transaction error
	QueryErrorCode                       // 103 query error
	RowsScanErrorCode                    // 104 row scan error
	SessionErrorCode                     // 105 db session error
	CommitErrorCode                      // 106 db commit error
	ParseErrorCode                       // 107 parser error
	LoadErrorCode                        // 108 loading error, e.g. load template
	GetIDErrorCode                       // 109 get id db error
	InsertErrorCode                      // 110 db insert error
	UpdateErrorCode                      // 111 update error
	LastInsertErrorCode                  // 112 db last insert error
	ValidateErrorCode                    // 113 validation error
	PatternErrorCode                     // 114 pattern error
	DecodeErrorCode                      // 115 decode error
	EncodeErrorCode                      // 116 encode error
	ContentTypeErrorCode                 // 117 content type error
	ParametersErrorCode                  // 118 parameters error
	NotImplementedApiCode                // 119 not implemented API error
	ReaderErrorCode                      // 120 io reader error
	WriterErrorCode                      // 121 io writer error
	UnmarshalErrorCode                   // 122 json unmarshal error
	MarshalErrorCode                     // 123 marshal error
	HttpRequestErrorCode                 // 124 HTTP request error
	MigrationErrorCode                   // 125 Migration error
	RemoveErrorCode                      // 126 remove error
	InvalidRequestErrorCode              // 127 invalid request error
)

// DBSError represents common structure for DBS errors
type DBSError struct {
	Reason   string `json:"reason"`   // error string
	Message  string `json:"message"`  // additional message describing the issue
	Function string `json:"function"` // DBS function
	Code     int    `json:"code"`     // DBS error code
}

// Error function implements details of DBS error message
func (e *DBSError) Error() string {
	return fmt.Sprintf(
		"<DBSError Code:%d Func:%s Msg:%s Error:%v>",
		e.Code, e.Function, e.Message, e.Reason)
}

// helper function to create dbs error
func Error(err error, code int, msg, function string) error {
	reason := "nil"
	if err != nil {
		reason = err.Error()
	}
	return &DBSError{
		Reason:   reason,
		Message:  msg,
		Code:     code,
		Function: function,
	}
}
