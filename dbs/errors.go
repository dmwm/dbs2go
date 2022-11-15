package dbs

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
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
	GenericErrorCode               = iota + 100 // generic DBS error
	DatabaseErrorCode                           // 101 database error
	TransactionErrorCode                        // 102 transaction error
	QueryErrorCode                              // 103 query error
	RowsScanErrorCode                           // 104 row scan error
	SessionErrorCode                            // 105 db session error
	CommitErrorCode                             // 106 db commit error
	ParseErrorCode                              // 107 parser error
	LoadErrorCode                               // 108 loading error, e.g. load template
	GetIDErrorCode                              // 109 get id db error
	InsertErrorCode                             // 110 db insert error
	UpdateErrorCode                             // 111 update error
	LastInsertErrorCode                         // 112 db last insert error
	ValidateErrorCode                           // 113 validation error
	PatternErrorCode                            // 114 pattern error
	DecodeErrorCode                             // 115 decode error
	EncodeErrorCode                             // 116 encode error
	ContentTypeErrorCode                        // 117 content type error
	ParametersErrorCode                         // 118 parameters error
	NotImplementedApiCode                       // 119 not implemented API error
	ReaderErrorCode                             // 120 io reader error
	WriterErrorCode                             // 121 io writer error
	UnmarshalErrorCode                          // 122 json unmarshal error
	MarshalErrorCode                            // 123 marshal error
	HttpRequestErrorCode                        // 124 HTTP request error
	MigrationErrorCode                          // 125 Migration error
	RemoveErrorCode                             // 126 remove error
	InvalidRequestErrorCode                     // 127 invalid request error
	BlockAlreadyExists                          // 128 block xxx already exists in DBS
	FileDataTypesDoesNotExist                   // 129 FileDataTypes does not exist in DBS
	FileParentDoesNotExist                      // 130 FileParent does not exist in DBS
	DatasetParentDoesNotExist                   // 131 DatasetParent does not exist in DBS
	ProcessedDatasetDoesNotExist                // 132 ProcessedDataset does not exist in DBS
	PrimaryDatasetTypeDoesNotExist              // 133 PrimaryDatasetType does not exist in DBS
	PrimaryDatasetDoesNotExist                  // 134 PrimaryDataset does not exist in DBS
	ProcessingEraDoesNotExist                   // 135 ProcessingEra does not exist in DBS
	AcquisitionEraDoesNotExist                  // 136 AcquisitionEra does not exist in DBS
	DataTierDoesNotExist                        // 137 DataTier does not exist in DBS
	PhysicsGroupDoesNotExist                    // 138 PhysicsGroup does not exist in DBS
	DatasetAccessTypeDoesNotExist               // 139 DatasetAccessType does not exist in DBS
	DatasetDoesNotExist                         // 140 Dataset does not exist in DBS
	LastAvailableErrorCode                      // last available DBS error code
)

// DBSError represents common structure for DBS errors
type DBSError struct {
	Reason     string `json:"reason"`     // error string
	Message    string `json:"message"`    // additional message describing the issue
	Function   string `json:"function"`   // DBS function
	Code       int    `json:"code"`       // DBS error code
	Stacktrace string `json:"stacktrace"` // Go stack trace
}

// Error function implements details of DBS error message
func (e *DBSError) Error() string {
	sep := ": "
	if strings.Contains(e.Reason, "DBSError") { // nested error
		sep += "nested "
	}
	return fmt.Sprintf(
		"DBSError Code:%d Description:%s Function:%s Message:%s Error%s%v",
		e.Code, e.Explain(), e.Function, e.Message, sep, e.Reason)
}

// ErrorStacktrace function implements details of DBS error message and stacktrace
func (e *DBSError) ErrorStacktrace() string {
	sep := ": "
	if strings.Contains(e.Reason, "DBSError") { // nested error
		sep += "nested "
	}
	return fmt.Sprintf(
		"DBSError Code:%d Description:%s Function:%s Message:%s Error%s%v Stacktrace: %v",
		e.Code, e.Explain(), e.Function, e.Message, sep, e.Reason, e.Stacktrace)
}

func (e *DBSError) Explain() string {
	switch e.Code {
	case GenericErrorCode:
		return "Generic DBS error"
	case DatabaseErrorCode:
		return "DBS DB error"
	case TransactionErrorCode:
		return "DBS DB transaction error"
	case QueryErrorCode:
		return "DBS DB query error, e.g. mailformed SQL statement"
	case RowsScanErrorCode:
		return "DBS DB row scane error, e.g. fail to get DB record from a database"
	case SessionErrorCode:
		return "DBS DB session error"
	case CommitErrorCode:
		return "DBS DB transaction commit error"
	case ParseErrorCode:
		return "DBS parser error, e.g. mailformed input parameter to the query"
	case LoadErrorCode:
		return "DBS file load error, e.g. fail to load DB template"
	case GetIDErrorCode:
		return "DBS DB ID error for provided entity, e.g. there is no record in DB for provided value"
	case InsertErrorCode:
		return "DBS DB insert record error"
	case UpdateErrorCode:
		return "DBS DB update record error"
	case LastInsertErrorCode:
		return "DBS DB laster insert record error, e.g. fail to obtain last inserted ID"
	case ValidateErrorCode:
		return "DBS validation error, e.g. input parameter does not match lexicon rules"
	case PatternErrorCode:
		return "DBS validation error when wrong pattern is provided"
	case DecodeErrorCode:
		return "DBS decode record failure, e.g. mailformed JSON"
	case EncodeErrorCode:
		return "DBS encode record failure, e.g. unable to convert structure to JSON"
	case ContentTypeErrorCode:
		return "Wrong Content-Type HTTP header in HTTP request"
	case ParametersErrorCode:
		return "DBS invalid parameter for the DBS API"
	case NotImplementedApiCode:
		return "DBS Not implemented API error"
	case ReaderErrorCode:
		return "DBS reader I/O error, e.g. unable to read HTTP POST payload"
	case WriterErrorCode:
		return "DBS writer I/O error, e.g. unable to write record to HTTP response"
	case UnmarshalErrorCode:
		return "DBS unable to parse JSON record"
	case MarshalErrorCode:
		return "DBS unable to convert record to JSON"
	case HttpRequestErrorCode:
		return "invalid HTTP request"
	case MigrationErrorCode:
		return "DBS Migration error"
	case RemoveErrorCode:
		return "Unable to remove record from DB"
	case InvalidRequestErrorCode:
		return "Invalid HTTP request"
	default:
		return "Not defined"
	}
	return "Not defined"
}

// helper function to create dbs error
func Error(err error, code int, msg, function string) error {
	reason := "nil"
	if err != nil {
		reason = err.Error()
	}
	stackSlice := make([]byte, 1024)
	s := runtime.Stack(stackSlice, false)
	return &DBSError{
		Reason:     reason,
		Message:    msg,
		Code:       code,
		Function:   function,
		Stacktrace: fmt.Sprintf("\n%s", stackSlice[0:s]),
	}
}
