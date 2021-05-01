package web

// handlers.go - provides handlers examples for dbs2go server

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

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
		tstamp := int64(start.UnixNano() / 1000000) // use milliseconds for MONIT
		status, dataSize, err := h(w, r)
		if err != nil {
			uri, e := url.QueryUnescape(r.RequestURI)
			if e != nil {
				log.Println("ERROR", err, r)
			} else {
				log.Println("ERROR", err, uri)
			}
		}
		logRequest(w, r, start, status, tstamp, dataSize)
	}
}

// responseMsg helper function to provide response to end-user
func responseMsg(w http.ResponseWriter, r *http.Request, msg, api string, code int) int64 {
	var out []dbs.Record
	rec := make(dbs.Record)
	rec["error"] = msg
	rec["api"] = api
	rec["method"] = r.Method
	rec["exception"] = code
	rec["type"] = "HTTPError"
	out = append(out, rec)
	data, _ := json.Marshal(out)
	w.WriteHeader(code)
	w.Write(data)
	return int64(len(data))
}

// helper function to extract user name or DN
func createBy(r *http.Request) string {
	cby := r.Header.Get("Cms-Authn-Login")
	if cby == "" {
		cby = r.Header.Get("Cms-Authn-Dn")
	}
	if cby == "" {
		return "DBS-workflow"
	}
	return cby
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

// ServerInfoHandler provides basic functionality of status response
func ServerInfoHandler(w http.ResponseWriter, r *http.Request) {
	//     var records []dbs.Record
	rec := make(dbs.Record)
	rec["server"] = Info()
	rec["dbs_version"] = "3.16.0-comp4"
	//     records = append(records, rec)
	//     data, err := json.Marshal(records)
	data, err := json.Marshal(rec)
	if err != nil {
		log.Fatalf("Fail to marshal records, %v", err)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// HelpHandler provides basic functionality of status response
func HelpHandler(w http.ResponseWriter, r *http.Request) {
	apis := []string{"blocksummaries", "help", "runsummaries", "parentDSTrio", "datatiers", "blockorigin", "blockTrio", "blockdump", "acquisitioneras", "filechildren", "fileparents", "serverinfo", "outputconfigs", "datasetchildren", "releaseversions", "files", "blocks", "physicsgroups", "filesummaries", "filelumis", "primarydstypes", "datasetparents", "datatypes", "processingeras", "runs", "datasets", "blockchildren", "primarydatasets", "acquisitioneras_ci", "blockparents", "datasetaccesstypes"}
	data, err := json.Marshal(apis)
	if err != nil {
		log.Fatalf("Fail to marshal records, %v", err)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// helper function to parse POST HTTP request payload
func parseParams(r *http.Request) (dbs.Record, error) {
	params := make(dbs.Record)
	// r.URL.Query() returns map[string][]string
	for k, values := range r.URL.Query() {
		var vals []string
		for _, v := range values {
			if strings.Contains(v, "[") {
				if strings.ToLower(k) == "run_num" {
					params["runList"] = true
				}
				v = v[1 : len(v)-1]
				for _, x := range strings.Split(v, ",") {
					x = strings.Trim(x, " ")
					x = strings.Replace(x, "'", "", -1)
					vals = append(vals, x)
				}
				continue
			}
			v = strings.Replace(v, "'", "", -1)
			vals = append(vals, v)
		}
		params[k] = vals
	}
	return params, nil
}

// helper function to parse POST HTTP request payload
func parsePayload(r *http.Request) (dbs.Record, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	params := make(dbs.Record)
	err := decoder.Decode(&params)
	if err != nil {
		return nil, err
	}
	for k, v := range params {
		s := fmt.Sprintf("%v", v)
		if strings.ToLower(k) == "run_num" && strings.Contains(s, "[") {
			params["runList"] = true
		}
		s = strings.Replace(s, "[", "", -1)
		s = strings.Replace(s, "]", "", -1)
		var out []string
		for _, vv := range strings.Split(s, " ") {
			ss := strings.Trim(vv, " ")
			if ss != "" {
				out = append(out, ss)
			}
		}
		if utils.VERBOSE > 1 {
			log.Printf("payload: key=%s val='%v' out=%v", k, v, out)
		}
		params[k] = out
	}
	return params, nil
}

// DBSPutHandler is a generic Post Handler to call DBS Post APIs
func DBSPutHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	params := make(dbs.Record)
	for k, v := range r.URL.Query() {
		// url query parameters are passed as list, we take first element only
		params[k] = v[0]
	}
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSPutHandler: API=%s, dn=%s, uri=%+v, params: %+v", a, dn, r.URL.RequestURI(), params)
	}
	params["create_by"] = createBy(r)
	var api dbs.API
	var err error
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSPutHandler: API=%s, dn=%s, uri=%+v", a, dn, r.URL.RequestURI())
	}
	if a == "acquisitioneras" {
		err = api.UpdateAcquisitionEras(params)
	} else if a == "datasets" {
		err = api.UpdateDatasets(params)
	} else if a == "blocks" {
		err = api.UpdateBlocks(params)
	} else if a == "files" {
		err = api.UpdateFiles(params)
	}
	if err != nil {
		size := responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
		return http.StatusInternalServerError, size, err
	}
	return http.StatusOK, 0, nil
}

// DBSPostHandler is a generic Post Handler to call DBS Post APIs
func DBSPostHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	headerContentType := r.Header.Get("Content-Type")
	if headerContentType != "application/json" {
		msg := fmt.Sprintf("unsupported Content-Type: '%s'", headerContentType)
		size := responseMsg(w, r, msg, "DBSPostHandler", http.StatusUnsupportedMediaType)
		return http.StatusUnsupportedMediaType, size, errors.New(msg)
	}
	defer r.Body.Close()
	var api dbs.API
	var err error
	var size int64
	var params dbs.Record
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSPostHandler: API=%s, dn=%s, uri=%+v", a, dn, r.URL.RequestURI())
	}
	cby := createBy(r)
	body := r.Body
	// handle gzip content encoding
	if r.Header.Get("Content-Encoding") == "gzip" {
		r.Header.Del("Content-Length")
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			log.Println("unable to get gzip reader", err)
			size := responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return http.StatusInternalServerError, size, err
		}
		body = utils.GzipReader{reader, r.Body}
	}
	if a == "datatiers" {
		err = api.InsertDataTiers(body, cby)
	} else if a == "outputconfigs" {
		err = api.InsertOutputConfigs(body, cby)
	} else if a == "primarydatasets" {
		err = api.InsertPrimaryDatasets(body, cby)
	} else if a == "acquisitioneras" {
		err = api.InsertAcquisitionEras(body, cby)
	} else if a == "processingeras" {
		err = api.InsertProcessingEras(body, cby)
	} else if a == "datasets" {
		err = api.InsertDatasets(body, cby)
	} else if a == "blocks" {
		err = api.InsertBlocks(body, cby)
	} else if a == "bulkblocks" {
		err = api.InsertBulkBlocks(body, cby)
	} else if a == "files" {
		err = api.InsertFiles(body, cby)
	} else if a == "fileparents" {
		err = api.InsertFileParents(body, cby)
	} else if a == "datasetlist" {
		params, err = parsePayload(r)
		if err != nil {
			size = responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return http.StatusInternalServerError, size, err
		}
		size, err = api.DatasetList(params, w)
	} else if a == "fileArray" {
		params, err = parsePayload(r)
		if utils.VERBOSE > 1 {
			log.Printf("fileArray payload: %+v", params)
		}
		if err != nil {
			size = responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return http.StatusInternalServerError, size, err
		}
		size, err = api.FileArray(params, w)
	} else if a == "fileparentsbylumi" {
		params, err = parsePayload(r)
		if err != nil {
			size = responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return http.StatusInternalServerError, size, err
		}
		size, err = api.FileParentsByLumi(params, w)
	} else if a == "filelumis" {
		params, err = parsePayload(r)
		if err != nil {
			size = responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return http.StatusInternalServerError, size, err
		}
		size, err = api.FileLumis(params, w)
	} else if a == "blockparents" {
		params, err = parsePayload(r)
		if err != nil {
			size = responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return http.StatusInternalServerError, size, err
		}
		size, err = api.BlockParents(params, w)
	}
	if err != nil {
		size = responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return http.StatusBadRequest, size, err
	}
	return http.StatusOK, 0, nil
}

// DBSGetHandler is a generic Get handler to call DBS Get APIs.
func DBSGetHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	status := http.StatusOK
	params, err := parseParams(r)
	if err != nil {
		return http.StatusBadRequest, 0, err
	}
	//     params := make(dbs.Record)
	//     for k, v := range r.URL.Query() {
	//         params[k] = v
	//     }
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSGetHandler: API=%s, dn=%s, uri=%+v, params: %+v", a, dn, r.URL.RequestURI(), params)
	}
	var api dbs.API
	//     var err error
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
	} else if a == "blocksummaries" {
		size, err = api.BlockSummaries(params, w)
	} else if a == "blockorigin" {
		size, err = api.BlockOrigin(params, w)
	} else if a == "datasetaccesstypes" {
		size, err = api.DatasetAccessTypes(params, w)
	} else {
		err = errors.New(fmt.Sprintf("not implemented API %s", api))
	}
	if err != nil {
		size := responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return http.StatusBadRequest, size, err
	}
	return status, size, nil
}

// NotImplementedHandler returns server status error
func NotImplemnetedHandler(w http.ResponseWriter, r *http.Request, api string) (int, int64, error) {
	log.Println("NotImplementedAPI", api)
	size := responseMsg(w, r, "not implemented", api, http.StatusInternalServerError)
	return http.StatusInternalServerError, size, nil
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
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "datasets")
	} else if r.Method == "PUT" {
		return DBSPutHandler(w, r, "datasets")
	}
	return DBSGetHandler(w, r, "datasets")
}

// BlocksHandler provides access to Blocks DBS API.
// Takes the following arguments: dataset, block_name, data_tier_name, origin_site_name, logical_file_name, run_num, min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, open_for_writing, detail
func BlocksHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "blocks")
	} else if r.Method == "PUT" {
		return DBSPutHandler(w, r, "blocks")
	}
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
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "files")
	} else if r.Method == "PUT" {
		return DBSPutHandler(w, r, "files")
	}
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
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "processingeras")
	}
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
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "acquisitioneras")
	} else if r.Method == "PUT" {
		return DBSPutHandler(w, r, "acquisitioneras")
	}
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
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "outputconfigs")
	}
	return DBSGetHandler(w, r, "outputconfigs")
}

// BlockParentsHandler provides access to BlockParents DBS API.
// Takes the following arguments: block_name
func BlockParentsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "blockparents")
	}
	return DBSGetHandler(w, r, "blockparents")
}

// FileLumisHandler provides access to FileLumis DBS API
// GET API takes the following arguments: logical_file_name, block_name, run_num, validFileOnly
// POST API takes no argument, the payload should be supplied as JSON
func FileLumisHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "filelumis")
	}
	return DBSGetHandler(w, r, "filelumis")
}

// FileArrayHandler provides access to FileArray DBS API
// POST API takes no argument, the payload should be supplied as JSON
func FileArrayHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSPostHandler(w, r, "fileArray")
}

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
	return DBSPostHandler(w, r, "bulkblocks")
}
