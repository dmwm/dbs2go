package web

// handlers.go - provides handlers examples for dbs2go server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/vkuznet/dbs2go/config"
	"github.com/vkuznet/dbs2go/dbs"
)

// helper to auth/authz incoming requests to the server
func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// perform authentication
		status := CMSAuth.CheckAuthnAuthz(r.Header)
		if !status {
			log.Printf("ERROR: fail to authenticate, HTTP headers %+v\n", r.Header)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if config.Config.Verbose > 2 {
			log.Printf("Auth layer status: %v headers: %+v\n", status, r.Header)
		}
		// Call the next handler
		next.ServeHTTP(w, r)
	})
}

// helper to validate incoming requests' parameters
func validateMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			next.ServeHTTP(w, r)
			return
		}
		// perform validation of input parameters
		err := dbs.Validate(r)
		if err != nil {
			log.Printf("ERROR: %v\n", err)
			w.WriteHeader(http.StatusBadRequest)
			rec := make(dbs.Record)
			rec["error"] = fmt.Sprintf("Validation error %v", err)
			if r, e := json.Marshal(rec); e == nil {
				w.Write(r)
			}
			return
		}
		// Call the next handler
		next.ServeHTTP(w, r)
	})
}

// LoggingHandlerFunc declares new handler function type which
// should return status (int) and error
type LoggingHandlerFunc func(w http.ResponseWriter, r *http.Request) (int, int64, error)

// LoggingHandler provides wrapper for any passed handler
// function. It executed given function and log its status and error
// to common logger
func LoggingHandler(h LoggingHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			atomic.AddUint64(&TotalPostRequests, 1)
		} else if r.Method == "GET" {
			atomic.AddUint64(&TotalGetRequests, 1)
		}
		start := time.Now()
		status, dataSize, err := h(w, r)
		if err != nil {
			log.Println("ERROR", err, h, r)
			w.WriteHeader(http.StatusInternalServerError)
		}
		tstamp := int64(start.UnixNano() / 1000000) // use milliseconds for MONIT
		logRequest(w, r, start, status, tstamp, dataSize)
	}
}

// MetricsHandler provides metrics
func MetricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(promMetrics()))
	return
}

// DummyHandler provides example how to write GET/POST handler
func DummyHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	// example of handling POST request
	if r.Method == "POST" {
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		rec := make(dbs.Record)
		status := http.StatusOK
		err := decoder.Decode(&rec)
		if err != nil {
			status = http.StatusInternalServerError
		}
		return status, 0, err
	}

	// example of handling GET request
	status := http.StatusOK
	params := make(dbs.Record)
	log.Printf("http request %+v", r)
	for k, v := range r.URL.Query() {
		params[k] = v
	}
	var api dbs.API
	records := api.Dummy(params)
	data, err := json.Marshal(records)
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	w.WriteHeader(status)
	w.Write(data)
	size := int64(binary.Size(data))
	return status, size, nil
}

// StatusHandler provides basic functionality of status response
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	var records []dbs.Record
	rec := make(dbs.Record)
	rec["status"] = http.StatusOK
	records = append(records, rec)
	data, err := json.Marshal(records)
	if err != nil {
		log.Fatalf("Fail to marshal records, %v", err)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// DBSPostHandler is a generic Post Handler to call DBS Post APIs
func DBSPostHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	headerContentType := r.Header.Get("Content-Type")
	if headerContentType != "application/json" {
		log.Println("Content-Type is not application/json")
		return http.StatusUnsupportedMediaType, 0, errors.New("unsupported Content-Type")
	}
	status := http.StatusOK
	args := make(dbs.Record)
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&args)
	if err != nil {
		log.Println("DBSPostHandler json Unmarshal error", err)
		return http.StatusInternalServerError, 0, err
	}
	params := make(dbs.Record)
	for k, v := range args {
		switch val := v.(type) {
		case string:
			params[k] = []string{val}
		case []string:
			params[k] = val
		case int:
			params[k] = []string{fmt.Sprintf("%d", val)}
		}
	}
	err = dbs.ValidatePostPayload(params)
	if err != nil {
		log.Println("DBSPostHandler invalid POST payload, error", err)
		return http.StatusInternalServerError, 0, err
	}
	var api dbs.API
	var size int64
	if a == "fileparentsbylumis" {
		size, err = api.FileParentByLumis(params, w)
	} else if a == "datasetlist" {
		size, err = api.DatasetList(params, w)
	} else if a == "fileArray" {
		size, err = api.FileArray(params, w)
	} else if a == "filelumis" {
		size, err = api.FileLumis(params, w)
	} else if a == "blockparents" {
		size, err = api.BlockParents(params, w)
	}
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	return status, size, nil
}

// DBSGetHandler is a generic Get handler to call DBS Get APIs.
func DBSGetHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	status := http.StatusOK
	params := make(dbs.Record)
	for k, v := range r.URL.Query() {
		params[k] = v
	}
	var api dbs.API
	var err error
	var size int64
	if a == "datatiers" {
		size, err = api.DataTiers(params, w)
	} else if a == "datasets" {
		size, err = api.Datasets(params, w)
	} else if a == "blocks" {
		size, err = api.Blocks(params, w)
	} else if a == "files" {
		size, err = api.Files(params, w)
	} else if a == "primarydatasets" {
		size, err = api.PrimaryDatasets(params, w)
	} else if a == "primarydstypes" {
		size, err = api.PrimaryDSTypes(params, w)
	} else if a == "acquisitioneras" {
		size, err = api.AcquisitionEras(params, w)
	} else if a == "acquisitioneras_ci" {
		size, err = api.AcquisitionErasCi(params, w)
	} else if a == "runsummaries" {
		size, err = api.RunSummaries(params, w)
	} else if a == "runs" {
		size, err = api.Runs(params, w)
	} else if a == "filechildren" {
		size, err = api.FileChildren(params, w)
	} else if a == "fileparents" {
		size, err = api.FileParents(params, w)
	} else if a == "outputconfigs" {
		size, err = api.OutputConfigs(params, w)
	} else if a == "datasetchildren" {
		size, err = api.DatasetChildren(params, w)
	} else if a == "releaseversions" {
		size, err = api.ReleaseVersions(params, w)
	} else if a == "physicsgroups" {
		size, err = api.PhysicsGroups(params, w)
	} else if a == "filesummaries" {
		size, err = api.FileSummaries(params, w)
	} else if a == "filelumis" {
		size, err = api.FileLumis(params, w)
	} else if a == "primarydstypes" {
		size, err = api.PrimaryDSTypes(params, w)
	} else if a == "datasetparents" {
		size, err = api.DatasetParents(params, w)
	} else if a == "datatypes" {
		size, err = api.DataTypes(params, w)
	} else if a == "processingeras" {
		size, err = api.ProcessingEras(params, w)
	} else if a == "blockchildren" {
		size, err = api.BlockChildren(params, w)
	} else if a == "blockparents" {
		size, err = api.BlockParents(params, w)
	} else if a == "datasetaccesstypes" {
		size, err = api.DatasetAccessTypes(params, w)
	} else {
		err = errors.New(fmt.Sprintf("not implemented API %s", api))
	}
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	return status, size, nil
}

// NotImplementedHandler returns server status error
func NotImplemnetedHandler(w http.ResponseWriter, r *http.Request, api string) (int, int64, error) {
	log.Println("NotImplementedAPI", api)
	return http.StatusInternalServerError, 0, nil
}

// DatatiersHandler provides access to DataTiers DBS API.
// Takes the following arguments: data_tier_name
func DatatiersHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "datatiers")
	}
	return DBSGetHandler(w, r, "datatiers")
}

// DatasetsHandler provides access to Datasets DBS API.
// Takes the following arguments: dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label, global_tag, processing_version, acquisition_era_name, run_num, physics_group_name, logical_file_name, primary_ds_name, primary_ds_type, processed_ds_name, data_tier_name, dataset_access_type, prep_id, create_by, last_modified_by, min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail, dataset_id
func DatasetsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datasets")
}

// BlocksHandler provides access to Blocks DBS API.
// Takes the following arguments: dataset, block_name, data_tier_name, origin_site_name, logical_file_name, run_num, min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, open_for_writing, detail
func BlocksHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blocks")
}

// BlockChildrenHandler provides access to BlockChildren DBS API.
// Takes the following arguments: block_name
func BlockChildrenHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blockchildren")
}

// BlockSummariesHandler provides access to BlockSummaries DBS API.
// Takes the following arguments: block_name, dataset, detail
func BlockSummariesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blocksummaries")
}

// BlockOriginHandler provides access to BlockOrigin DBS API.
// Takes the following arguments: origin_site_name, dataset, block_name
func BlockOriginHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blockorigin")
}

// FilesHandler provides access to Files DBS API.
// Takes the following arguments: dataset, block_name, logical_file_name, release_version, pset_hash, app_name, output_module_label, run_num, origin_site_name, lumi_list, detail, validFileOnly, sumOverLumi
func FilesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "files")
}

// FileChildrenHandler provides access to FileChildren DBS API.
// Takes the following arguments: logical_file_name, block_name, block_id
func FileChildrenHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "filechildren")
}

// FileParentsHandler provides access to FileParent DBS API.
// Takes the following arguments: logical_file_name, block_id, block_name
func FileParentsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "fileparents")
}

// FileSummariesHandler provides access to FileSummaries DBS API.
// Takes the following arguments: block_name, dataset, run_num, validFileOnly, sumOverLumi
func FileSummariesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "filesummaries")
}

// RunsHandler provides access to Runs DBS API.
// Takes the following arguments: run_num, logical_file_name, block_name, dataset
func RunsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "runs")
}

// RunSummariesHandler provides access to RunSummaries DBS API.
// Takes the following arguments: dataset, run_num
func RunSummariesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "runsummaries")
}

//ProcessingErasHandler provices access to ProcessingEras DBS API.
// Takes the following arguments: processing_version
func ProcessingErasHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "processingeras")
}

// PrimaryDSTypesHandler provides access to PrimaryDSTypes DBS API.
// Takes the following arguments: primary_ds_type, dataset
func PrimaryDSTypesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "primarydstypes")
}

// DataTypesHandler provides access to DataTypes DBS API.
// Takes the following arguments: datatype, dataset
func DataTypesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datatypes")
}

// ReleaseVersionsHandler provides access to ReleaseVersions DBS API.
// Takes the following arguments: release_version, dataset, logical_file_name
func ReleaseVersionsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "releaseversions")
}

// AcquisitionErasHandler provides access to AcquisitionEras DBS API.
// Takes the following arguments: acquisition_era_name
func AcquisitionErasHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "acquisitioneras")
}

// AcquisitionErasCiHandler provides access to AcquisitionErasCi DBS API.
// Takes the following arguments: acquisition_era_name
func AcquisitionErasCiHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return NotImplemnetedHandler(w, r, "acquisitionerasci")
}

// ParentDSTrioHandler provides access to ParentDSTrio DBS API.
// Takes the following arguments: acquisition_era_name
func ParentDSTrioHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return NotImplemnetedHandler(w, r, "parentdstrio")
}

// PrimaryDatasetsHandler provides access to PrimaryDatasets DBS API.
// Takes the following arguments: primary_ds_name, primary_ds_type
func PrimaryDatasetsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "primarydatasets")
}

// DatasetParentsHandler provides access to DatasetParents DBS API.
// Takes the following arguments: dataset
func DatasetParentsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datasetparents")
}

// DatasetChildrenHandler provides access to DatasetChildren DBS API.
// Takes the following arguments: dataset
func DatasetChildrenHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datasetchildren")
}

// DatasetAccessTypesHandler provides access to DatasetAccessTypes DBS API.
// Takes the following arguments: dataset_access_type
func DatasetAccessTypesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datasetaccesstypes")
}

// PhysicsGroupsHandler provides access to PhysicsGroups DBS API
// Takes the following arguments: physics_group_name
func PhysicsGroupsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "physicsgroups")
}

// OutputConfigsHandler provides access to OutputConfigs DBS API.
// Takes the following arguments: dataset, logical_file_name, release_version, pset_hash, app_name, output_module_label, block_id, global_tag
func OutputConfigsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "outputconfigs")
}

// BlockParentsHandler provides access to BlockParents DBS API.
// Takes the following arguments: block_name
func BlockParentsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blockparent")
}

// FileLumisHandler provides access to FileLumis DBS API
// GET API takes the following arguments: logical_file_name, block_name, run_num, validFileOnly
// POST API takes no argument, the payload should be supplied as JSON
func FileLumisHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "filelumis")
	}
	return DBSPostHandler(w, r, "filelumis")
}

// FileArrayHandler provides access to FileArray DBS API
// POST API takes no argument, the payload should be supplied as JSON
func FileArrayHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSPostHandler(w, r, "fileArray")
}

// func FileArrayHandler(w http.ResponseWriter, r *http.Request) {
//     log.Println("request", r)
//     defer r.Body.Close()
//     decoder := json.NewDecoder(r.Body)
//     params := make(dbs.Record)
//     err := decoder.Decode(&params)
//     if err != nil {
//         log.Println("FileArrayHandler error", err)
//         w.WriteHeader(http.StatusInternalServerError)
//         return
//     }
//     var api dbs.API
//     size, err := api.FileArray(params, w)
//     if err != nil {
//         log.Println("FileArrayHandler error", err)
//         w.WriteHeader(http.StatusInternalServerError)
//         return
//     }
//     log.Println("size", size)
// }

// DatasteListHandler provides access to DatasetList DBS API
// POST API takes no argument, the payload should be supplied as JSON
func DatasetListHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSPostHandler(w, r, "datasetlist")
}

// FileParentsByLumiHandler provides access to FileParentsByLumi DBS API
// POST API takes no argument, the payload should be supplied as JSON
func FileParentsByLumiHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSPostHandler(w, r, "fileparentsbylumi")
}

// BulkBlocksHandler provides access to BulkBlocks DBS API
// POST API takes no argument, the payload should be supplied as JSON
func BulkBlocksHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	var api dbs.API
	err := api.BulkBlocks(decoder)
	if err != nil {
		log.Println("BulkBlocksHandler error", err)
		return http.StatusInternalServerError, 0, err
	}
	return http.StatusOK, 0, nil
}
