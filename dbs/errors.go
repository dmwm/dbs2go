package dbs

import (
	"errors"
	"fmt"
	"runtime"
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
	// generic errors
	GenericErrorCode          = iota + 100 // generic DBS error
	DatabaseErrorCode                      // 101 database error
	TransactionErrorCode                   // 102 transaction error
	QueryErrorCode                         // 103 query error
	RowsScanErrorCode                      // 104 row scan error
	SessionErrorCode                       // 105 db session error
	CommitErrorCode                        // 106 db commit error
	ParseErrorCode                         // 107 parser error
	LoadErrorCode                          // 108 loading template error
	GetIDErrorCode                         // 109 get id db error
	InsertErrorCode                        // 110 db insert error
	UpdateErrorCode                        // 111 update error
	LastInsertErrorCode                    // 112 db last insert error
	ValidateErrorCode                      // 113 validation error
	InvalidPatternErrorCode                // 114 invalid pattern error
	DecodeErrorCode                        // 115 decode error
	EncodeErrorCode                        // 116 encode error
	ContentTypeErrorCode                   // 117 content type error
	InvalidParameterErrorCode              // 118 invalid JSON payload parameters or validation error
	NotImplementedApiCode                  // 119 not implemented API error
	ReaderErrorCode                        // 120 io reader error
	WriterErrorCode                        // 121 io writer error
	UnmarshalErrorCode                     // 122 JSON unmarshal (deserialization) error
	MarshalErrorCode                       // 123 JSON marshal (serialization) error
	HttpRequestErrorCode                   // 124 HTTP request error
	X509ProxyErrorCode                     // 127 X509 proxy error code

	// logical errors
	BlockAlreadyExists             = iota + 200 // 200 block xxx already exists in DBS
	FileDataTypesDoesNotExist                   // 201 FileDataTypes does not exist in DBS
	FileParentDoesNotExist                      // 202 FileParent does not exist in DBS
	DatasetParentDoesNotExist                   // 203 DatasetParent does not exist in DBS
	ProcessedDatasetDoesNotExist                // 204 ProcessedDataset does not exist in DBS
	PrimaryDatasetTypeDoesNotExist              // 205 PrimaryDatasetType does not exist in DBS
	PrimaryDatasetDoesNotExist                  // 206 PrimaryDataset does not exist in DBS
	ProcessingEraDoesNotExist                   // 207 ProcessingEra does not exist in DBS
	AcquisitionEraDoesNotExist                  // 208 AcquisitionEra does not exist in DBS
	DataTierDoesNotExist                        // 209 DataTier does not exist in DBS
	PhysicsGroupDoesNotExist                    // 210 PhysicsGroup does not exist in DBS
	DatasetAccessTypeDoesNotExist               // 211 DatasetAccessType does not exist in DBS
	DatasetDoesNotExist                         // 212 Dataset does not exist in DBS

	// insert errors
	InsertDatasetErrorCode                = iota + 300 // 300 insert error for dataset
	InsertDatasetParentErrorCode                       // 301 insert error for dataset parents
	InsertDatasetOutputModConfigErrorCode              // 302 insert error for DatasetOutputModConfigs
	InsertDatasetConfigurationsErrorCode               // 303 insert error for dataset configurations
	InsertDatasetAccessTypeErrorCode                   // 304 insert error for dataset access types
	InsertBlockErrorCode                               // 305 insert error for block
	InsertBlockParentErrorCode                         // 306 insert error for block parents
	InsertBlockStatsErrorCode                          // 307 insert error for block stats
	InsertBulkblockErrorCode                           // 308 insert error for bulkblocks transaction
	InsertBlockDumpErrorCode                           // 309 insert error for block dump
	InsertFileErrorCode                                // 310 insert error for file
	InsertFileLumiErrorCode                            // 311 insert error for file lumis
	InsertFileOutputModConfigErrorCode                 // 312 insert error for file output mod config
	InsertFileParentErrorCode                          // 313 insert error for file parents
	InsertPrimaryDatasetErrorCode                      // 314 insert error for primary dataset
	InsertPrimaryDatasetTypeErrorCode                  // 315 insert error for primary dataset type
	InsertAcquisitionEraErrorCode                      // 316 insert error for acquisition eras
	InsertOutputConfigErrorCode                        // 317 insert error for output config
	InsertProcessedDatasetErrorCode                    // 318 insert error for processed dataset
	InsertApplicationExecutableErrorCode               // 319 insert error for application executable
	InsertFileDataTypeErrorCode                        // 320 insert error for file data type
	InsertMigrationBlockErrorCode                      // 321 insert error for migration block
	InsertMigrationRequestErrorCode                    // 322 insert error for migration request
	InsertParameterSetHashErrorCode                    // 323 insert error for parameter set hash
	InsertReleaseVersionErrorCode                      // 324 insert error for release version
	InsertPhysicsGroupErrorCode                        // 325 insert error for physics group
	InsertProcessingEraErrorCode                       // 326 insert error for processing era
	InsertDataTierErrorCode                            // 327 insert error for data tier

	// Missing data error codes, e.g. during insertion of specific error we do not find
	// proper foreign key relationship (missing error)
	GetBlockIDErrorCode              = iota + 400 // 400 fail to get block ID
	GetFileIDErrorCode                            // 401 fail to get find file id
	GetFileDataTypesIDErrorCode                   // 402 fail to get file data type id
	GetPrimaryDatasetIDErrorCode                  // 403 fail to get primary dataset id
	GetProcessingEraIDErrorCode                   // 404 fail to get processing era id
	GetAcquisitionEraIDErrorCode                  // 405 fail to get acquisition era id
	GetDatatierIDErrorCode                        // 406 fail to get datatier id
	GetPhysicsGroupIDErrorCode                    // 407 fail to get physics group id
	GetDatasetAccessTypeIDErrorCode               // 408 fail to get dataset access type id
	GetProcessedDatasetIDErrorCode                // 409 fail to get processed dataset id
	GetDatasetIDErrorCode                         // 410 fail to get dataset id
	GetPrimaryDSIDErrorCode                       // 411 fail to get primary dataset id
	GetDataTierIDErrorCode                        // 412 fail to get data tier id
	GetOutputModConfigIDErrorCode                 // 413 fail to get output mod config id
	GetPrimaryDatasetTypeIDErrorCode              // 414 fail to get primary dataset types id
	GetFileDataTypeIDErrorCode                    // 415 fail to get file data type id
	GetDatasetParentIDErrorCode                   // 416 fail to get dataset parent id

	// update operation errors
	UpdateAcquisitionEraErrorCode = iota + 500 // 500 update acquisition era error
	UpdateBlockErrorCode                       // 501 update block error
	UpdateDatasetErrorCode                     // 502 update dataset error
	UpdateFileErrorCode                        // 503 update file error

	// migration errors
	UpdateMigrationErrorCode  = iota + 600 // 600 update migration error
	RemoveMigrationErrorCode               // 601 remove migration error
	CancelMigrationErrorCode               // 602 cancel migration error
	CleanupMigrationErrorCode              // 603 cleanup migration error
	MigrationErrorCode                     // 604 Migration error
	RemoveErrorCode                        // 605 remove error

	LastAvailableErrorCode = iota + 900 // last available DBS error code
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
	return fmt.Sprintf(
		"\nDBSError\n   Code: %d\n   Description: %s\n   Function: %s\n   Message: %s\n   Reason: %v\n",
		e.Code, e.Explain(), e.Function, e.Message, e.Reason)
}

// ErrorStacktrace function implements details of DBS error message and stacktrace
func (e *DBSError) ErrorStacktrace() string {
	return fmt.Sprintf(
		"\nDBSError Stacktrace\n   Code: %d\n   Description: %s\n   Function: %s\n   Message: %s\n   Reason: %v\nStacktrace: %v\n\n",
		e.Code, e.Explain(), e.Function, e.Message, e.Reason, e.Stacktrace)
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
		return "DBS DB query error, e.g. malformed SQL statement"
	case RowsScanErrorCode:
		return "DBS DB row scane error, e.g. fail to get DB record from a database"
	case SessionErrorCode:
		return "DBS DB session error"
	case CommitErrorCode:
		return "DBS DB transaction commit error"
	case ParseErrorCode:
		return "DBS parser error, e.g. malformed input parameter to the query"
	case LoadErrorCode:
		return "DBS file load error, e.g. fail to load SQL template"
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
	case InvalidParameterErrorCode:
		return "DBS invalid parameter for the DBS API or validation error"
	case InvalidPatternErrorCode:
		return "invalid pattern is provided"
	case DecodeErrorCode:
		return "DBS decode record failure, e.g. malformed JSON"
	case EncodeErrorCode:
		return "DBS encode record failure, e.g. unable to convert structure to JSON"
	case ContentTypeErrorCode:
		return "Wrong Content-Type HTTP header in HTTP request"
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
	case X509ProxyErrorCode:
		return "X509 proxy error, e.g. expired certificate"

	case BlockAlreadyExists:
		return "block already exists"
	case FileDataTypesDoesNotExist:
		return "file data type does not exit"
	case FileParentDoesNotExist:
		return "file parent does not exist"
	case DatasetParentDoesNotExist:
		return "dataset parent does not exist"
	case ProcessedDatasetDoesNotExist:
		return "processed dataset does not exist"
	case PrimaryDatasetTypeDoesNotExist:
		return "primary dataset type does not exist"
	case PrimaryDatasetDoesNotExist:
		return "primary dataset does not exist"
	case ProcessingEraDoesNotExist:
		return "processing era does not exist"
	case AcquisitionEraDoesNotExist:
		return "acquisition era does not exist"
	case DataTierDoesNotExist:
		return "data tier does not exist"
	case PhysicsGroupDoesNotExist:
		return "physics group does not exist"
	case DatasetAccessTypeDoesNotExist:
		return "dataset access type does not exist"
	case DatasetDoesNotExist:
		return "dataset does not exist"

	// insert error codes
	case InsertDatasetErrorCode:
		return "insert dataset error"
	case InsertDatasetParentErrorCode:
		return "insert dataset parent error"
	case InsertDatasetOutputModConfigErrorCode:
		return "insert dataset output mod config error"
	case InsertDatasetConfigurationsErrorCode:
		return "insert dataset config error"
	case InsertDatasetAccessTypeErrorCode:
		return "insert dataset access type error"
	case InsertBlockErrorCode:
		return "insert block error"
	case InsertBlockParentErrorCode:
		return "insert block parent error"
	case InsertBlockStatsErrorCode:
		return "insert block stats error"
	case InsertBulkblockErrorCode:
		return "insert bulkblock error"
	case InsertBlockDumpErrorCode:
		return "insert block dump error"
	case InsertFileErrorCode:
		return "insert file error"
	case InsertFileLumiErrorCode:
		return "insert file lumi error"
	case InsertFileOutputModConfigErrorCode:
		return "insert file output mod config error"
	case InsertFileParentErrorCode:
		return "insert file parent error"
	case InsertPrimaryDatasetErrorCode:
		return "insert primary dataset error"
	case InsertPrimaryDatasetTypeErrorCode:
		return "insert primary dataset type error"
	case InsertAcquisitionEraErrorCode:
		return "insert acquisition era error"
	case InsertOutputConfigErrorCode:
		return "insert output config error"
	case InsertApplicationExecutableErrorCode:
		return "insert application executable error"
	case InsertFileDataTypeErrorCode:
		return "insert file data type error"
	case InsertMigrationBlockErrorCode:
		return "insert migration block error"
	case InsertMigrationRequestErrorCode:
		return "insert migration request error"
	case InsertParameterSetHashErrorCode:
		return "insert parameter set hash error"
	case InsertReleaseVersionErrorCode:
		return "insert release version error"
	case InsertPhysicsGroupErrorCode:
		return "insert physics group error"
	case InsertProcessingEraErrorCode:
		return "insert processing era error"
	case InsertDataTierErrorCode:
		return "insert data tier error"

	// transient errors at DB level
	case GetBlockIDErrorCode:
		return "unable to get block id"
	case GetFileIDErrorCode:
		return "unable to get file id"
	case GetFileDataTypesIDErrorCode:
		return "unable to get file data type id"
	case GetPrimaryDatasetIDErrorCode:
		return "unable to get primary dataset id"
	case GetProcessingEraIDErrorCode:
		return "unable to get processing era id"
	case GetAcquisitionEraIDErrorCode:
		return "unable to get acquisition era id"
	case GetDatatierIDErrorCode:
		return "unable to get data tier id"
	case GetPhysicsGroupIDErrorCode:
		return "unable to get physics group id"
	case GetDatasetAccessTypeIDErrorCode:
		return "unable to get dataset access type id"
	case GetProcessedDatasetIDErrorCode:
		return "unable to get processed dataset id"
	case GetDatasetIDErrorCode:
		return "unable to get dataset id"
	case GetPrimaryDSIDErrorCode:
		return "unable to get primary dataset id"
	case GetDataTierIDErrorCode:
		return "unable to get data tier id"
	case GetOutputModConfigIDErrorCode:
		return "unable to get output mod config id"
	case GetPrimaryDatasetTypeIDErrorCode:
		return "unable to get primary dataset type id"
	case GetFileDataTypeIDErrorCode:
		return "unable to get file data type id"
	case GetDatasetParentIDErrorCode:
		return "unable to get dataset parent id"

	// update operation codes
	case UpdateAcquisitionEraErrorCode:
		return "fail to update acquisition era table"
	case UpdateBlockErrorCode:
		return "fail to update block table"
	case UpdateDatasetErrorCode:
		return "fail to update dataset table"
	case UpdateFileErrorCode:
		return "fail to update file table"

	// migration errors
	case MigrationErrorCode:
		return "migration error"
	case RemoveErrorCode:
		return "remove record error"
	case UpdateMigrationErrorCode:
		return "update migration error"
	case RemoveMigrationErrorCode:
		return "remove migration error"
	case CancelMigrationErrorCode:
		return "cancel migration error"
	case CleanupMigrationErrorCode:
		return "cleanup migration error"

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

// GetDBSErrors returns map of DBS errors
func GetDBSErrors() map[int]string {
	errors := make(map[int]string)
	for i := GenericErrorCode; i < LastAvailableErrorCode; i++ {
		err := &DBSError{Code: i}
		explain := err.Explain()
		if explain != "Not defined" {
			errors[i] = explain
		}
	}
	return errors
}
