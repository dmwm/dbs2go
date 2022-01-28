package dbs

import (
	"fmt"
)

// DBS Error codes provides static representation of DBS errors, they cover 1xx range
const (
	GenericError      = iota + 100 // generic DBS error
	DatabaseError                  // 101 database error
	TransactionError               // 102 transaction error
	QueryError                     // 103 query error
	RowsScanError                  // 104 row scan error
	SessionError                   // 105 db session error
	CommitError                    // 106 db commit error
	ParseError                     // 107 parser error
	LoadError                      // 108 loading error, e.g. load template
	GetIDError                     // 109 get id db error
	InsertError                    // 110 db insert error
	LastInsertError                // 111 db last insert error
	ValidateError                  // 112 validation error
	PatternError                   // 113 pattern error
	DecodeError                    // 114 decode error
	EncodeError                    // 115 encode error
	ContentTypeError               // 116 content type error
	ParametersError                // 117 parameters error
	NotImplementedApi              // 118 not implemented API error
	ReaderError                    // 119 io reader error
	WriterError                    // 120 io writer error
	UnmarshalError                 // 121 json unmarshal error
	MarshalError                   // 122 marshal error
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
	return fmt.Sprintf("<DBSError Code:%d Func:%s Msg:%s Error:%v>", e.Code, e.Function, e.Message, e.Reason)
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
