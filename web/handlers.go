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

// responseMsg helper function to provide response to end-user
func responseMsg(w http.ResponseWriter, r *http.Request, msg, api string, code int) int64 {
	//     var out []dbs.Record
	rec := make(dbs.Record)
	rec["error"] = msg
	rec["api"] = api
	rec["method"] = r.Method
	rec["exception"] = code
	rec["type"] = "HTTPError"
	//     out = append(out, rec)
	//     data, _ := json.Marshal(out)
	data, _ := json.Marshal(rec)
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
	params := make(dbs.Record)
	log.Printf("http request %+v", r)
	for k, v := range r.URL.Query() {
		params[k] = v
	}
	var api dbs.API
	records := api.Dummy(params)
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
	rec["server"] = Info()
	// TODO: replace with something and check that it satisfy with
	// DBS test107 regex r'^(3+\.[0-9]+\.[0-9]+[\.\-a-z0-9]*$)'
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
func DBSPutHandler(w http.ResponseWriter, r *http.Request, a string) {
	// all outputs will be added to output list
	//     sep := ",\n"
	if r.Header.Get("Accept") == "application/ndjson" {
		//         sep = "\n"
	} else {
		w.Write([]byte("[\n"))
		defer w.Write([]byte("]\n"))
	}

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
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
		return
	}
}

// DBSPostHandler is a generic Post Handler to call DBS Post APIs
func DBSPostHandler(w http.ResponseWriter, r *http.Request, a string) {
	// all outputs will be added to output list
	sep := ",\n"
	if r.Header.Get("Accept") == "application/ndjson" {
		sep = "\n"
	} else {
		w.Write([]byte("[\n"))
		defer w.Write([]byte("]\n"))
	}

	headerContentType := r.Header.Get("Content-Type")
	if headerContentType != "application/json" {
		msg := fmt.Sprintf("unsupported Content-Type: '%s'", headerContentType)
		responseMsg(w, r, msg, "DBSPostHandler", http.StatusUnsupportedMediaType)
		return
	}
	defer r.Body.Close()
	var api dbs.API
	var err error
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
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
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
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
		}
		err = api.DatasetList(params, sep, w)
	} else if a == "fileArray" {
		params, err = parsePayload(r)
		if utils.VERBOSE > 1 {
			log.Printf("fileArray payload: %+v", params)
		}
		if err != nil {
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
		}
		err = api.FileArray(params, sep, w)
	} else if a == "fileparentsbylumi" {
		params, err = parsePayload(r)
		if err != nil {
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
		}
		err = api.FileParentsByLumi(params, sep, w)
	} else if a == "filelumis" {
		params, err = parsePayload(r)
		if err != nil {
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
		}
		err = api.FileLumis(params, sep, w)
	} else if a == "blockparents" {
		params, err = parsePayload(r)
		if err != nil {
			responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusInternalServerError)
			return
		}
		err = api.BlockParents(params, sep, w)
	} else if a == "submit" {
		err = api.Submit(body, cby, w)
	} else if a == "remove" {
		err = api.Remove(body, cby, w)
	}
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return
	}
}

// DBSGetHandler is a generic Get handler to call DBS Get APIs.
func DBSGetHandler(w http.ResponseWriter, r *http.Request, a string) {
	// all outputs will be added to output list
	sep := ",\n"
	if r.Header.Get("Accept") == "application/ndjson" {
		sep = "\n"
	} else {
		w.Write([]byte("[\n"))
		defer w.Write([]byte("]\n"))
	}

	params, err := parseParams(r)
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return
	}
	//     params := make(dbs.Record)
	//     for k, v := range r.URL.Query() {
	//         params[k] = v
	//     }
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		uri, err := url.QueryUnescape(r.URL.RequestURI())
		if err != nil {
			log.Println("unable to unescape request uri", err)
			uri = r.RequestURI
		}
		log.Printf("DBSGetHandler: API=%s, dn=%s, uri=%+v, params: %+v", a, dn, uri, params)
	}
	var api dbs.API
	if a == "datatiers" {
		err = api.DataTiers(params, sep, w)
	} else if a == "datasets" {
		err = api.Datasets(params, sep, w)
	} else if a == "blocks" {
		err = api.Blocks(params, sep, w)
	} else if a == "files" {
		err = api.Files(params, sep, w)
	} else if a == "primarydatasets" {
		err = api.PrimaryDatasets(params, sep, w)
	} else if a == "primarydstypes" {
		err = api.PrimaryDSTypes(params, sep, w)
	} else if a == "acquisitioneras" {
		err = api.AcquisitionEras(params, sep, w)
	} else if a == "acquisitioneras_ci" {
		err = api.AcquisitionErasCi(params, sep, w)
	} else if a == "runsummaries" {
		err = api.RunSummaries(params, sep, w)
	} else if a == "runs" {
		err = api.Runs(params, sep, w)
	} else if a == "filechildren" {
		err = api.FileChildren(params, sep, w)
	} else if a == "fileparents" {
		err = api.FileParents(params, sep, w)
	} else if a == "outputconfigs" {
		err = api.OutputConfigs(params, sep, w)
	} else if a == "datasetchildren" {
		err = api.DatasetChildren(params, sep, w)
	} else if a == "releaseversions" {
		err = api.ReleaseVersions(params, sep, w)
	} else if a == "physicsgroups" {
		err = api.PhysicsGroups(params, sep, w)
	} else if a == "filesummaries" {
		err = api.FileSummaries(params, sep, w)
	} else if a == "filelumis" {
		err = api.FileLumis(params, sep, w)
	} else if a == "primarydstypes" {
		err = api.PrimaryDSTypes(params, sep, w)
	} else if a == "datasetparents" {
		err = api.DatasetParents(params, sep, w)
	} else if a == "datatypes" {
		err = api.DataTypes(params, sep, w)
	} else if a == "processingeras" {
		err = api.ProcessingEras(params, sep, w)
	} else if a == "blockchildren" {
		err = api.BlockChildren(params, sep, w)
	} else if a == "blockparents" {
		err = api.BlockParents(params, sep, w)
	} else if a == "blocksummaries" {
		err = api.BlockSummaries(params, sep, w)
	} else if a == "blockorigin" {
		err = api.BlockOrigin(params, sep, w)
	} else if a == "blockTrio" {
		err = api.BlockFileLumiIds(params, sep, w)
	} else if a == "parentDSTrio" {
		err = api.ParentDatasetFileLumiIds(params, sep, w)
	} else if a == "datasetaccesstypes" {
		err = api.DatasetAccessTypes(params, sep, w)
	} else if a == "status" {
		err = api.Status(params, sep, w)
	} else {
		err = errors.New(fmt.Sprintf("not implemented API %s", api))
	}
	if err != nil {
		responseMsg(w, r, fmt.Sprintf("%v", err), a, http.StatusBadRequest)
		return
	}
}

// NotImplementedHandler returns server status error
func NotImplemnetedHandler(w http.ResponseWriter, r *http.Request, api string) {
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
	DBSGetHandler(w, r, "fileparents")
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
	NotImplemnetedHandler(w, r, "acquisitionerasci")
}

// PrimaryDatasetsHandler provides access to PrimaryDatasets DBS API.
// Takes the following arguments: primary_ds_name, primary_ds_type
func PrimaryDatasetsHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "primarydatasets")
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

// DatasteListHandler provides access to DatasetList DBS API
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
	DBSPostHandler(w, r, "bulkblocks")
}

// Migration server handlers

// MigrateSubmitHandler provides access to Submit DBS API
// POST API takes no argument, the payload should be supplied as JSON
func MigrateSubmitHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "submit")
}

// MigrateRemoveHandler provides access to Remove DBS API
// POST API takes no argument, the payload should be supplied as JSON
func MigrateRemoveHandler(w http.ResponseWriter, r *http.Request) {
	DBSPostHandler(w, r, "remove")
}

// MigrateStatusHandler provides access to Status DBS API
func MigrateStatusHandler(w http.ResponseWriter, r *http.Request) {
	DBSGetHandler(w, r, "status")
}
