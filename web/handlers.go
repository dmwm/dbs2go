package web

// handlers.go - provides handlers examples for dbs2go server

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// helper function to get request URI
func requestURI(r *http.Request) string {
	uri, err := url.QueryUnescape(r.RequestURI)
	if err != nil {
		log.Println("unable to unescape request uri", r.RequestURI, "error", err)
		uri = r.RequestURI
	}
	return uri
}

// responseMsg helper function to provide response to end-user
func responseMsg(w http.ResponseWriter, r *http.Request, msg, api string, code int) int64 {
	rec := make(dbs.Record)
	rec["error"] = msg
	rec["api"] = api
	rec["method"] = r.Method
	rec["exception"] = code
	rec["type"] = "HTTPError"
	//     data, _ := json.Marshal(rec)
	var out []dbs.Record
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

// QueryHandler provides access to graph ql query
func QueryHandler(w http.ResponseWriter, r *http.Request) {
	var params struct {
		Query         string                 `json:"query"`
		OperationName string                 `json:"operationName"`
		Variables     map[string]interface{} `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response := GraphQLSchema.Exec(r.Context(), params.Query, params.OperationName, params.Variables)
	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

// DummyHandler provides example how to write GET/POST handler
func DummyHandler(w http.ResponseWriter, r *http.Request) {
	// example of handling POST request
	if r.Method == "POST" {
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		rec := make(dbs.Record)
		err := decoder.Decode(&rec)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		return
	}

	// example of handling GET request
	status := http.StatusOK
	params, err := parseParams(r)
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), "dummy", http.StatusBadRequest)
		return
	}
	api := &dbs.API{
		Params: params,
		Api:    "dummy",
	}
	if utils.VERBOSE > 0 {
		log.Println(api.String())
	}
	records := api.Dummy()
	data, err := json.Marshal(records)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	w.Write(data)
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
	rec["server"] = ServerInfo
	// DBS test107 regex r'^(3+\.[0-9]+\.[0-9]+[\.\-a-z0-9]*$)'
	rec["dbs_version"] = GitVersion
	//     records = append(records, rec)
	//     data, err := json.Marshal(records)
	data, err := json.Marshal(rec)
	if err != nil {
		log.Fatalf("Fail to marshal records, %v", err)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// ApisHandler provides list of supporter apis by the DBS server
func ApisHandler(w http.ResponseWriter, r *http.Request) {
	data, err := json.Marshal(webRoutes)
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
func DBSPutHandler(w http.ResponseWriter, r *http.Request, a string) {
	// all outputs will be added to output list
	sep := ","
	if r.Header.Get("Accept") == "application/ndjson" {
		sep = ""
	}

	params := make(dbs.Record)
	for k, v := range r.URL.Query() {
		// url query parameters are passed as list, we take first element only
		params[k] = v[0]
	}
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSPutHandler: API=%s, dn=%s, uri=%s, params: %+v", a, dn, requestURI(r), params)
	}
	cby := createBy(r)
	params["create_by"] = cby
	api := &dbs.API{
		Params:    params,
		CreateBy:  cby,
		Api:       a,
		Separator: sep,
	}
	if utils.VERBOSE > 0 {
		log.Println(api.String())
	}
	var err error
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSPutHandler: API=%s, dn=%s, uri=%s", a, dn, requestURI(r))
	}
	if a == "acquisitioneras" {
		err = api.UpdateAcquisitionEras()
	} else if a == "datasets" {
		err = api.UpdateDatasets()
	} else if a == "blocks" {
		err = api.UpdateBlocks()
	} else if a == "files" {
		err = api.UpdateFiles()
	}
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
		return
	}
}

// DBSPostHandler is a generic Post Handler to call DBS Post APIs
func DBSPostHandler(w http.ResponseWriter, r *http.Request, a string) {
	// all outputs will be added to output list
	sep := ","
	if r.Header.Get("Accept") == "application/ndjson" {
		sep = ""
	}

	headerContentType := r.Header.Get("Content-Type")
	if headerContentType != "application/json" {
		msg := fmt.Sprintf("unsupported Content-Type: '%s'", headerContentType)
		responseMsg(w, r, msg, "DBSPostHandler", http.StatusUnsupportedMediaType)
		return
	}
	defer r.Body.Close()
	var err error
	var params dbs.Record
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSPostHandler: API=%s, dn=%s, uri=%s", a, dn, requestURI(r))
	}
	cby := createBy(r)
	body := r.Body
	// handle gzip content encoding
	if r.Header.Get("Content-Encoding") == "gzip" {
		r.Header.Del("Content-Length")
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			log.Println("unable to get gzip reader", err)
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
		}
		body = utils.GzipReader{reader, r.Body}
	}
	api := &dbs.API{
		Reader:    body,
		Writer:    w,
		Params:    params,
		Separator: sep,
		CreateBy:  cby,
		Api:       a,
	}
	if a == "fileArray" || a == "datasetlist" || a == "fileparentsbylumi" || a == "filelumis" || a == "blockparents" {
		params, err = parsePayload(r)
		if err != nil {
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
		}
		api.Params = params
	}
	if utils.VERBOSE > 0 {
		log.Println(api.String())
	}
	if a == "datatiers" {
		err = api.InsertDataTiers()
	} else if a == "outputconfigs" {
		err = api.InsertOutputConfigs()
	} else if a == "primarydatasets" {
		err = api.InsertPrimaryDatasets()
	} else if a == "acquisitioneras" {
		err = api.InsertAcquisitionEras()
	} else if a == "processingeras" {
		err = api.InsertProcessingEras()
	} else if a == "datasets" {
		err = api.InsertDatasets()
	} else if a == "blocks" {
		err = api.InsertBlocks()
	} else if a == "bulkblocks" {
		err = api.InsertBulkBlocks()
	} else if a == "files" {
		err = api.InsertFiles()
	} else if a == "fileparents" {
		err = api.InsertFileParents()
	} else if a == "datasetlist" {
		err = api.DatasetList()
	} else if a == "fileArray" {
		err = api.FileArray()
	} else if a == "fileparentsbylumi" {
		err = api.FileParentsByLumi()
	} else if a == "filelumis" {
		err = api.FileLumis()
	} else if a == "blockparents" {
		err = api.BlockParents()
	} else if a == "submit" {
		err = api.SubmitMigration()
	} else if a == "process" {
		err = api.ProcessMigration(dbs.MigrationProcessTimeout, true) // write process report
	} else if a == "remove" {
		err = api.RemoveMigration()
	}
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return
	}
}

// DBSGetHandler is a generic Get handler to call DBS Get APIs.
//gocyclo:ignore
func DBSGetHandler(w http.ResponseWriter, r *http.Request, a string) {
	// all outputs will be added to output list
	sep := ","
	if r.Header.Get("Accept") == "application/ndjson" {
		sep = ""
	}

	params, err := parseParams(r)
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return
	}
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSGetHandler: API=%s, dn=%s, uri=%+v, params: %+v", a, dn, requestURI(r), params)
	}
	api := &dbs.API{
		Writer:    w,
		Params:    params,
		Separator: sep,
		Api:       a,
	}
	if utils.VERBOSE > 0 {
		log.Println(api.String())
	}
	if a == "datatiers" {
		err = api.DataTiers()
	} else if a == "datasets" {
		err = api.Datasets()
	} else if a == "blocks" {
		err = api.Blocks()
	} else if a == "blockdump" {
		err = api.BlockDump()
	} else if a == "files" {
		err = api.Files()
	} else if a == "primarydatasets" {
		err = api.PrimaryDatasets()
	} else if a == "primarydstypes" {
		err = api.PrimaryDSTypes()
	} else if a == "acquisitioneras" {
		err = api.AcquisitionEras()
	} else if a == "acquisitioneras_ci" {
		err = api.AcquisitionErasCi()
	} else if a == "runsummaries" {
		err = api.RunSummaries()
	} else if a == "runs" {
		err = api.Runs()
	} else if a == "filechildren" {
		err = api.FileChildren()
	} else if a == "fileparents" {
		err = api.FileParents()
	} else if a == "outputconfigs" {
		err = api.OutputConfigs()
	} else if a == "datasetchildren" {
		err = api.DatasetChildren()
	} else if a == "releaseversions" {
		err = api.ReleaseVersions()
	} else if a == "physicsgroups" {
		err = api.PhysicsGroups()
	} else if a == "filesummaries" {
		err = api.FileSummaries()
	} else if a == "filelumis" {
		err = api.FileLumis()
	} else if a == "primarydstypes" {
		err = api.PrimaryDSTypes()
	} else if a == "datasetparents" {
		err = api.DatasetParents()
	} else if a == "datatypes" {
		err = api.DataTypes()
	} else if a == "processingeras" {
		err = api.ProcessingEras()
	} else if a == "blockchildren" {
		err = api.BlockChildren()
	} else if a == "blockparents" {
		err = api.BlockParents()
	} else if a == "blocksummaries" {
		err = api.BlockSummaries()
	} else if a == "blockorigin" {
		err = api.BlockOrigin()
	} else if a == "blockTrio" {
		err = api.BlockFileLumiIds()
	} else if a == "parentDSTrio" {
		err = api.ParentDatasetFileLumiIds()
	} else if a == "datasetaccesstypes" {
		err = api.DatasetAccessTypes()
	} else if a == "status" {
		err = api.StatusMigration()
	} else if a == "total" {
		err = api.TotalMigration()
	} else {
		msg := fmt.Sprintf("not implemented API %s", api)
		err = errors.New(msg)
	}
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return
	}
}

// NotImplementedHandler returns server status error
func NotImplementedHandler(w http.ResponseWriter, r *http.Request, api string) {
	log.Println("NotImplementedAPI", api)
	responseMsg(w, r, "not implemented", api, http.StatusInternalServerError)
}

// DatatiersHandler provides access to DataTiers DBS API.
// Takes the following arguments: data_tier_name
func DatatiersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "datatiers")
	} else {
		DBSGetHandler(w, r, "datatiers")
	}
}

// DatasetsHandler provides access to Datasets DBS API.
// Takes the following arguments: dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label, global_tag, processing_version, acquisition_era_name, run_num, physics_group_name, logical_file_name, primary_ds_name, primary_ds_type, processed_ds_name, data_tier_name, dataset_access_type, prep_id, create_by, last_modified_by, min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail, dataset_id
func DatasetsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "datasets")
	} else if r.Method == "PUT" {
		DBSPutHandler(w, r, "datasets")
	} else {
		DBSGetHandler(w, r, "datasets")
	}
}

// ParentDSTrioHandler provides access to ParentDSTrio DBS API.
// Takes the following arguments: dataset
func ParentDSTrioHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "parentDSTrio")
}

// BlocksHandler provides access to Blocks DBS API.
// Takes the following arguments: dataset, block_name, data_tier_name, origin_site_name, logical_file_name, run_num, min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, open_for_writing, detail
func BlocksHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "blocks")
	} else if r.Method == "PUT" {
		DBSPutHandler(w, r, "blocks")
	} else {
		DBSGetHandler(w, r, "blocks")
	}
}

// BlockDumpHandler provides access to BlockDump DBS API
func BlockDumpHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "blockdump")
}

// BlockChildrenHandler provides access to BlockChildren DBS API.
// Takes the following arguments: block_name
func BlockChildrenHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "blockchildren")
}

// BlockTrioHandler provides access to BlockTrio DBS API.
// Takes the following arguments: block_name, list of lfns
func BlockTrioHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "blockTrio")
}

// BlockSummariesHandler provides access to BlockSummaries DBS API.
// Takes the following arguments: block_name, dataset, detail
func BlockSummariesHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "blocksummaries")
}

// BlockOriginHandler provides access to BlockOrigin DBS API.
// Takes the following arguments: origin_site_name, dataset, block_name
func BlockOriginHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "blockorigin")
}

// FilesHandler provides access to Files DBS API.
// Takes the following arguments: dataset, block_name, logical_file_name, release_version, pset_hash, app_name, output_module_label, run_num, origin_site_name, lumi_list, detail, validFileOnly, sumOverLumi
func FilesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "files")
	} else if r.Method == "PUT" {
		DBSPutHandler(w, r, "files")
	} else {
		DBSGetHandler(w, r, "files")
	}
}

// FileChildrenHandler provides access to FileChildren DBS API.
// Takes the following arguments: logical_file_name, block_name, block_id
func FileChildrenHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "filechildren")
}

// FileParentsHandler provides access to FileParent DBS API.
// Takes the following arguments: logical_file_name, block_id, block_name
func FileParentsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "fileparents")
	} else {
		DBSGetHandler(w, r, "fileparents")
	}
}

// FileSummariesHandler provides access to FileSummaries DBS API.
// Takes the following arguments: block_name, dataset, run_num, validFileOnly, sumOverLumi
func FileSummariesHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "filesummaries")
}

// RunsHandler provides access to Runs DBS API.
// Takes the following arguments: run_num, logical_file_name, block_name, dataset
func RunsHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "runs")
}

// RunSummariesHandler provides access to RunSummaries DBS API.
// Takes the following arguments: dataset, run_num
func RunSummariesHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "runsummaries")
}

//ProcessingErasHandler provices access to ProcessingEras DBS API.
// Takes the following arguments: processing_version
func ProcessingErasHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "processingeras")
	} else {
		DBSGetHandler(w, r, "processingeras")
	}
}

// PrimaryDSTypesHandler provides access to PrimaryDSTypes DBS API.
// Takes the following arguments: primary_ds_type, dataset
func PrimaryDSTypesHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "primarydstypes")
}

// DataTypesHandler provides access to DataTypes DBS API.
// Takes the following arguments: datatype, dataset
func DataTypesHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "datatypes")
}

// ReleaseVersionsHandler provides access to ReleaseVersions DBS API.
// Takes the following arguments: release_version, dataset, logical_file_name
func ReleaseVersionsHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "releaseversions")
}

// AcquisitionErasHandler provides access to AcquisitionEras DBS API.
// Takes the following arguments: acquisition_era_name
func AcquisitionErasHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "acquisitioneras")
	} else if r.Method == "PUT" {
		DBSPutHandler(w, r, "acquisitioneras")
	} else {
		DBSGetHandler(w, r, "acquisitioneras")
	}
}

// AcquisitionErasCiHandler provides access to AcquisitionErasCi DBS API.
// Takes the following arguments: acquisition_era_name
func AcquisitionErasCiHandler(w http.ResponseWriter, r *http.Request) {
	NotImplementedHandler(w, r, "acquisitionerasci")
}

// PrimaryDatasetsHandler provides access to PrimaryDatasets DBS API.
// Takes the following arguments: primary_ds_name, primary_ds_type
func PrimaryDatasetsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "primarydatasets")
	} else {
		DBSGetHandler(w, r, "primarydatasets")
	}
}

// DatasetParentsHandler provides access to DatasetParents DBS API.
// Takes the following arguments: dataset
func DatasetParentsHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "datasetparents")
}

// DatasetChildrenHandler provides access to DatasetChildren DBS API.
// Takes the following arguments: dataset
func DatasetChildrenHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "datasetchildren")
}

// DatasetAccessTypesHandler provides access to DatasetAccessTypes DBS API.
// Takes the following arguments: dataset_access_type
func DatasetAccessTypesHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "datasetaccesstypes")
}

// PhysicsGroupsHandler provides access to PhysicsGroups DBS API
// Takes the following arguments: physics_group_name
func PhysicsGroupsHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "physicsgroups")
}

// OutputConfigsHandler provides access to OutputConfigs DBS API.
// Takes the following arguments: dataset, logical_file_name, release_version, pset_hash, app_name, output_module_label, block_id, global_tag
func OutputConfigsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "outputconfigs")
	} else {
		DBSGetHandler(w, r, "outputconfigs")
	}
}

// BlockParentsHandler provides access to BlockParents DBS API.
// Takes the following arguments: block_name
func BlockParentsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "blockparents")
	} else {
		DBSGetHandler(w, r, "blockparents")
	}
}

// FileLumisHandler provides access to FileLumis DBS API
// GET API takes the following arguments: logical_file_name, block_name, run_num, validFileOnly
// POST API takes no argument, the payload should be supplied as JSON
func FileLumisHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "filelumis")
	} else {
		DBSGetHandler(w, r, "filelumis")
	}
}

// FileArrayHandler provides access to FileArray DBS API
// POST API takes no argument, the payload should be supplied as JSON
func FileArrayHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "fileArray")
}

// DatasetListHandler provides access to DatasetList DBS API
// POST API takes no argument, the payload should be supplied as JSON
func DatasetListHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "datasetlist")
}

// FileParentsByLumiHandler provides access to FileParentsByLumi DBS API
// POST API takes no argument, the payload should be supplied as JSON
func FileParentsByLumiHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "fileparentsbylumi")
}

// BulkBlocksHandler provides access to BulkBlocks DBS API
// POST API takes no argument, the payload should be supplied as JSON
func BulkBlocksHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		DBSPostHandler(w, r, "bulkblocks")
	} else {
		DBSGetHandler(w, r, "bulkblocks")
	}
}

// Migration server handlers

// MigrationSubmitHandler provides access to Submit DBS API
// POST API takes no argument, the payload should be supplied as JSON
func MigrationSubmitHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "submit")
}

// MigrationProcessHandler provides access to Process DBS API
// POST API takes no argument, the payload should be supplied as JSON
func MigrationProcessHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "process")
}

// MigrationRemoveHandler provides access to Remove DBS API
// POST API takes no argument, the payload should be supplied as JSON
func MigrationRemoveHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "remove")
}

// MigrationStatusHandler provides access to Status DBS API
func MigrationStatusHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "status")
}

// MigrationTotalHandler provides access to Status DBS API
func MigrationTotalHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "total")
}
