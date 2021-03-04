package dbs

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Files DBS API
func (API) Files(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	var lumigen, rungen, lfngen bool
	var sumOverLumi string

	if len(params) == 0 {
		msg := "Files API with empty parameter map"
		return dbsError(w, msg)
	}
	// When sumOverLumi=1, no lfn list or run_num list allowed
	if _, ok := params["sumOverLumi"]; ok {
		arr := getValues(params, "sumOverLumi")
		if len(arr) != 1 {
			return dbsError(w, "sumOverLumi has more than one value")
		}
		sumOverLumi = arr[0]
		if vals, ok := params["run_num"]; ok {
			runs := fmt.Sprintf("%v", vals)
			if strings.Contains(runs, ",") || strings.Contains(runs, "-") {
				msg := "When sumOverLumi=1, no run_num list allowed"
				return dbsError(w, msg)
			}
		} else if vals, ok := params["logical_file_name"]; ok {
			lfns := fmt.Sprintf("%v", vals)
			if strings.Contains(lfns, ",") {
				msg := "When sumOverLumi=1, no lfn list or run_num list allowed"
				return dbsError(w, msg)
			}
		}
	}

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Addition"] = false
	tmpl["RunNumber"] = false
	tmpl["LumiList"] = false
	tmpl["Addition"] = false

	lumis := getValues(params, "lumi_list")
	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}
	if len(runs) > 0 {
		tmpl["RunNumber"] = true
	}

	if len(lumis) > 0 {
		tmpl["LumiList"] = true
	}
	// files API does not supprt run_num=1 when no lumi
	if len(runs) == 1 && len(lumis) == 0 && runs[0] == "1" {
		msg := "files API does not supprt run_num=1 when no lumi"
		return dbsError(w, msg)
	}

	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		_, val := OperatorValue(validFileOnly[0])
		if val == "1" {
			cond := "F.IS_FILE_VALID = 1"
			conds = append(conds, cond)
			cond = "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')"
			conds = append(conds, cond)
		} else if val == "0" {
			cond := "F.IS_FILE_VALID <> -1"
			conds = append(conds, cond)
		}
	}

	conds, args = AddParam("dataset", "D.DATASET", params, conds, args)
	conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	if _, e := getSingleValue(params, "release_version"); e == nil {
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(params, "pset_hash"); e == nil {
		conds, args = AddParam("pset_hash", "PSH.PSET_HASH", params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(params, "app_name"); e == nil {
		conds, args = AddParam("app_name", "AEX.APP_NAME", params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(params, "output_module_label"); e == nil {
		conds, args = AddParam("output_module_label", "OMC.OUTPUT_MODULE_LABEL", params, conds, args)
		tmpl["Addition"] = true
	}
	conds, args = AddParam("origin_site_name", "B.ORIGIN_SITE_NAME", params, conds, args)

	// load our SQL statement
	stm, err := LoadTemplateSQL("files", tmpl)
	if err != nil {
		return 0, err
	}

	// add lfns conditions
	lfns := getValues(params, "logical_file_name")
	if len(lfns) > 1 {
		lfngen = true
		token, binds := TokenGenerator(lfns, 100, "lfns_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := " F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
	}
	// add run conditions
	if len(runs) > 1 {
		rungen = true
		token, whereRuns, bindsRuns := runsClause("FL", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	} else if len(runs) == 1 {
		conds, args = AddParam("run_num", "FL.RUN_NUM", params, conds, args)
	}

	// add lumis conditions
	if len(lumis) > 1 {
		lumigen = true
		token, binds := TokenGenerator(lumis, 4000, "lumis_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := " FL.LUMI_SECTION_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
		tmpl["LumiGenerator"] = token
	} else if len(lumis) == 1 {
		conds, args = AddParam("lumi_list", "FL.LUMI_SECTION_NUM", params, conds, args)
	}

	if (rungen && lfngen) || (lumigen && lfngen) || (rungen && lumigen) {
		msg := "cannot supply more than one list (lfn, run_num or lumi) at one query"
		return dbsError(w, msg)
	}

	// check sumOverLumi
	if sumOverLumi == "1" {
		stm = strings.Replace(stm, "F.EVENT_COUNT,", "", -1)
		tmpl["Statement"] = stm
		stm, err = LoadTemplateSQL("files_sumoverlumi", tmpl)
		if err != nil {
			return 0, err
		}
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFiles DBS API
func (API) InsertFiles(r io.Reader) (int64, error) {
	// TODO: implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSFile.py
	/*
	        :param qInserts: True means that inserts will be queued instead of done immediately. INSERT QUEUE
	 Manager will perform the inserts, within few minutes.
	        :type qInserts: bool
	        :param logical_file_name (required) : string
	        :param is_file_valid: (optional, default = 1): 1/0
	        :param block, required: /a/b/c#d
	        :param dataset, required: /a/b/c
	        :param file_type (optional, default = EDM): one of the predefined types,
	        :param check_sum (optional): string
	        :param event_count (optional, default = -1): int
	        :param file_size (optional, default = -1.): float
	        :param adler32 (optional): string
	        :param md5 (optional): string
	        :param auto_cross_section (optional, default = -1.): float
	        :param file_lumi_list (optional, default = []): [{'run_num': 123, 'lumi_section_num': 12},{}....]
	        :param file_parent_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....]
	        :param file_assoc_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....]
	        :param file_output_config_list(optional, default = []) :
	        [{'app_name':..., 'release_version':..., 'pset_hash':...., output_module_label':...},{}.....]
	*/
	// logic:
	// dataset_id = self.datasetid.execute(conn, dataset=f["dataset"])
	// dsconfigs = [x['output_mod_config_id'] for x in self.dsconfigids.execute(conn, dataset=f["dataset"])]
	// block_info = self.blocklist.execute(conn, block_name=f["block_name"])
	// file_type_id = self.ftypeid.execute( conn, f.get("file_type", "EDM"))
	// self.filein.execute(conn, filein, transaction=tran)
	// fcdao["output_mod_config_id"] = self.outconfigid.execute(conn, fc["app_name"],
	// self.flumiin.execute(conn, flumis2insert, transaction=tran)
	// self.fparentin.execute(conn, fparents2insert, transaction=tran)
	// self.fconfigin.execute(conn, fconfigs2insert, transaction=tran)
	// self.blkparentin.execute(conn, bkParentage2insert, transaction=tran)
	// self.dsparentin.execute(conn, dsParentage2insert, transaction=tran)
	// blkParams = self.blkstats.execute(conn, block_id, transaction=tran)
	// self.blkstatsin.execute(conn, blkParams, transaction=tran)

	//     return InsertValues("insert_files", values)
	return 0, nil
}
