--------------------------------------------------------
--  File created - Wednesday-December-09-2020   
--------------------------------------------------------

--------------------------------------------------------
--  DDL for Table ACQUISITION_ERAS
--------------------------------------------------------

  CREATE TABLE "ACQUISITION_ERAS" 
   (	"ACQUISITION_ERA_ID" INTEGER, 
	"ACQUISITION_ERA_NAME" VARCHAR2(120), 
	"START_DATE" INTEGER, 
	"END_DATE" INTEGER, 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"DESCRIPTION" VARCHAR2(40)
   ) ;
--------------------------------------------------------
--  DDL for Table APPLICATION_EXECUTABLES
--------------------------------------------------------

  CREATE TABLE "APPLICATION_EXECUTABLES" 
   (	"APP_EXEC_ID" INTEGER, 
	"APP_NAME" VARCHAR2(100)
   ) ;
--------------------------------------------------------
--  DDL for Table ASSOCIATED_FILES
--------------------------------------------------------

  CREATE TABLE "ASSOCIATED_FILES" 
   (	"ASSOCATED_FILE_ID" INTEGER, 
	"THIS_FILE_ID" INTEGER, 
	"ASSOCATED_FILE" INTEGER
   ) ;
--------------------------------------------------------
--  DDL for Table BLOCKS
--------------------------------------------------------

  CREATE TABLE "BLOCKS" 
   (	"BLOCK_ID" INTEGER, 
	"BLOCK_NAME" VARCHAR2(500), 
	"DATASET_ID" INTEGER, 
	"OPEN_FOR_WRITING" INTEGER DEFAULT 1, 
	"ORIGIN_SITE_NAME" VARCHAR2(200), 
	"BLOCK_SIZE" INTEGER, 
	"FILE_COUNT" INTEGER, 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"LAST_MODIFICATION_DATE" INTEGER, 
	"LAST_MODIFIED_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table BLOCK_PARENTS
--------------------------------------------------------

  CREATE TABLE "BLOCK_PARENTS" 
   (	"THIS_BLOCK_ID" INTEGER , 
	"PARENT_BLOCK_ID" INTEGER, 
	 CONSTRAINT "PK_BP" PRIMARY KEY ("THIS_BLOCK_ID", "PARENT_BLOCK_ID")
   ) ;
--------------------------------------------------------
--  DDL for Table BRANCH_HASHES
--------------------------------------------------------

  CREATE TABLE "BRANCH_HASHES" 
   (	"BRANCH_HASH_ID" INTEGER, 
	"BRANCH_HASH" VARCHAR2(700), 
	"CONTENT" CLOB
   ) ;
--------------------------------------------------------
--  DDL for Table DATASETS
--------------------------------------------------------

  CREATE TABLE "DATASETS" 
   (	"DATASET_ID" INTEGER, 
	"DATASET" VARCHAR2(700), 
	"IS_DATASET_VALID" INTEGER DEFAULT 1, 
	"PRIMARY_DS_ID" INTEGER, 
	"PROCESSED_DS_ID" INTEGER, 
	"DATA_TIER_ID" INTEGER, 
	"DATASET_ACCESS_TYPE_ID" INTEGER, 
	"ACQUISITION_ERA_ID" INTEGER, 
	"PROCESSING_ERA_ID" INTEGER, 
	"PHYSICS_GROUP_ID" INTEGER, 
	"XTCROSSSECTION" FLOAT(126), 
	"PREP_ID" VARCHAR2(256), 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"LAST_MODIFICATION_DATE" INTEGER, 
	"LAST_MODIFIED_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table DATASET_ACCESS_TYPES
--------------------------------------------------------

  CREATE TABLE "DATASET_ACCESS_TYPES" 
   (	"DATASET_ACCESS_TYPE_ID" INTEGER, 
	"DATASET_ACCESS_TYPE" VARCHAR2(100)
   ) ;
--------------------------------------------------------
--  DDL for Table DATASET_OUTPUT_MOD_CONFIGS
--------------------------------------------------------

  CREATE TABLE "DATASET_OUTPUT_MOD_CONFIGS" 
   (	"DS_OUTPUT_MOD_CONF_ID" INTEGER, 
	"DATASET_ID" INTEGER, 
	"OUTPUT_MOD_CONFIG_ID" INTEGER
   ) ;
--------------------------------------------------------
--  DDL for Table DATASET_PARENTS
--------------------------------------------------------

  CREATE TABLE "DATASET_PARENTS" 
   (	"THIS_DATASET_ID" INTEGER, 
	"PARENT_DATASET_ID" INTEGER, 
	 CONSTRAINT "PK_DP" PRIMARY KEY ("THIS_DATASET_ID", "PARENT_DATASET_ID")
   ) ;
--------------------------------------------------------
--  DDL for Table DATASET_RUNS
--------------------------------------------------------

  CREATE TABLE "DATASET_RUNS" 
   (	"DATASET_RUN_ID" INTEGER, 
	"DATASET_ID" INTEGER, 
	"RUN_NUMBER" INTEGER, 
	"COMPLETE" INTEGER DEFAULT 0, 
	"LUMI_SECTION_COUNT" INTEGER, 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table DATA_TIERS
--------------------------------------------------------

  CREATE TABLE "DATA_TIERS" 
   (	"DATA_TIER_ID" INTEGER, 
	"DATA_TIER_NAME" VARCHAR2(100), 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table DBS_VERSIONS
--------------------------------------------------------

  CREATE TABLE "DBS_VERSIONS" 
   (	"DBS_VERSION_ID" INTEGER, 
	"SCHEMA_VERSION" VARCHAR2(40), 
	"DBS_RELEASE_VERSION" VARCHAR2(40), 
	"INSTANCE_NAME" VARCHAR2(40), 
	"INSTANCE_TYPE" VARCHAR2(40), 
	"CREATION_DATE" INTEGER, 
	"LAST_MODIFICATION_DATE" INTEGER
   ) ;
--------------------------------------------------------
--  DDL for Table FILES
--------------------------------------------------------

  CREATE TABLE "FILES" 
   (	"FILE_ID" INTEGER, 
	"LOGICAL_FILE_NAME" VARCHAR2(500), 
	"IS_FILE_VALID" INTEGER DEFAULT 1, 
	"DATASET_ID" INTEGER, 
	"BLOCK_ID" INTEGER, 
	"FILE_TYPE_ID" INTEGER, 
	"CHECK_SUM" VARCHAR2(100), 
	"EVENT_COUNT" INTEGER, 
	"FILE_SIZE" INTEGER, 
	"BRANCH_HASH_ID" INTEGER, 
	"ADLER32" VARCHAR2(100) DEFAULT NULL, 
	"MD5" VARCHAR2(100) DEFAULT NULL, 
	"AUTO_CROSS_SECTION" FLOAT(126), 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"LAST_MODIFICATION_DATE" INTEGER, 
	"LAST_MODIFIED_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table FILE_DATA_TYPES
--------------------------------------------------------

  CREATE TABLE "FILE_DATA_TYPES" 
   (	"FILE_TYPE_ID" INTEGER, 
	"FILE_TYPE" VARCHAR2(100)
   ) ;
--------------------------------------------------------
--  DDL for Table FILE_LUMIS
--------------------------------------------------------

  CREATE TABLE "FILE_LUMIS" 
   (	"RUN_NUM" INTEGER, 
	"LUMI_SECTION_NUM" INTEGER, 
	"FILE_ID" INTEGER, 
	"EVENT_COUNT" INTEGER
   ) ;
--------------------------------------------------------
--  DDL for Table FILE_OUTPUT_MOD_CONFIGS
--------------------------------------------------------

  CREATE TABLE "FILE_OUTPUT_MOD_CONFIGS" 
   (	"FILE_OUTPUT_CONFIG_ID" INTEGER, 
	"FILE_ID" INTEGER, 
	"OUTPUT_MOD_CONFIG_ID" INTEGER
   ) ;
--------------------------------------------------------
--  DDL for Table FILE_PARENTS
--------------------------------------------------------

  CREATE TABLE "FILE_PARENTS" 
   (	"THIS_FILE_ID" INTEGER, 
	"PARENT_FILE_ID" INTEGER, 
	 CONSTRAINT "PK_FP" PRIMARY KEY ("THIS_FILE_ID", "PARENT_FILE_ID")
   ) ;
--------------------------------------------------------
--  DDL for Table MIGRATION_BLOCKS
--------------------------------------------------------

  CREATE TABLE "MIGRATION_BLOCKS" 
   (	"MIGRATION_BLOCK_ID" INTEGER, 
	"MIGRATION_REQUEST_ID" INTEGER, 
	"MIGRATION_BLOCK_NAME" VARCHAR2(700), 
	"MIGRATION_ORDER" INTEGER, 
	"MIGRATION_STATUS" INTEGER, 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"LAST_MODIFICATION_DATE" INTEGER, 
	"LAST_MODIFIED_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table MIGRATION_REQUESTS
--------------------------------------------------------

  CREATE TABLE "MIGRATION_REQUESTS" 
   (	"MIGRATION_REQUEST_ID" INTEGER, 
	"MIGRATION_URL" VARCHAR2(300), 
	"MIGRATION_INPUT" VARCHAR2(700), 
	"MIGRATION_STATUS" INTEGER, 
	"MIGRATION_SERVER" VARCHAR2(100),
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"LAST_MODIFICATION_DATE" INTEGER, 
	"LAST_MODIFIED_BY" VARCHAR2(500), 
	"RETRY_COUNT" INTEGER
   ) ;
--------------------------------------------------------
--  DDL for Table OUTPUT_MODULE_CONFIGS
--------------------------------------------------------

  CREATE TABLE "OUTPUT_MODULE_CONFIGS" 
   (	"OUTPUT_MOD_CONFIG_ID" INTEGER, 
	"APP_EXEC_ID" INTEGER, 
	"RELEASE_VERSION_ID" INTEGER, 
	"PARAMETER_SET_HASH_ID" INTEGER, 
	"OUTPUT_MODULE_LABEL" VARCHAR2(100) DEFAULT 'NONE',
	"GLOBAL_TAG" VARCHAR2(255), 
	"SCENARIO" VARCHAR2(40), 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table PARAMETER_SET_HASHES
--------------------------------------------------------

  CREATE TABLE "PARAMETER_SET_HASHES" 
   (	"PARAMETER_SET_HASH_ID" INTEGER, 
	"PSET_HASH" VARCHAR2(128), 
	"PSET_NAME" VARCHAR2(135)
   ) ;
--------------------------------------------------------
--  DDL for Table PHYSICS_GROUPS
--------------------------------------------------------

  CREATE TABLE "PHYSICS_GROUPS" 
   (	"PHYSICS_GROUP_ID" INTEGER, 
	"PHYSICS_GROUP_NAME" VARCHAR2(100)
   ) ;
--------------------------------------------------------
--  DDL for Table PRIMARY_DATASETS
--------------------------------------------------------

  CREATE TABLE "PRIMARY_DATASETS" 
   (	"PRIMARY_DS_ID" INTEGER, 
	"PRIMARY_DS_NAME" VARCHAR2(100), 
	"PRIMARY_DS_TYPE_ID" INTEGER, 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table PRIMARY_DS_TYPES
--------------------------------------------------------

  CREATE TABLE "PRIMARY_DS_TYPES" 
   (	"PRIMARY_DS_TYPE_ID" INTEGER, 
	"PRIMARY_DS_TYPE" VARCHAR2(100)
   ) ;
--------------------------------------------------------
--  DDL for Table PROCESSED_DATASETS
--------------------------------------------------------

  CREATE TABLE "PROCESSED_DATASETS" 
   (	"PROCESSED_DS_ID" INTEGER, 
	"PROCESSED_DS_NAME" VARCHAR2(235)
   ) ;
--------------------------------------------------------
--  DDL for Table PROCESSING_ERAS
--------------------------------------------------------

  CREATE TABLE "PROCESSING_ERAS" 
   (	"PROCESSING_ERA_ID" INTEGER, 
	"PROCESSING_VERSION" INTEGER, 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"DESCRIPTION" VARCHAR2(40)
   ) ;
--------------------------------------------------------
--  DDL for Table RELEASE_VERSIONS
--------------------------------------------------------

  CREATE TABLE "RELEASE_VERSIONS" 
   (	"RELEASE_VERSION_ID" INTEGER, 
	"RELEASE_VERSION" VARCHAR2(100)
   ) ;
--------------------------------------------------------
--  DDL for Table TOAD_PLAN_TABLE
--------------------------------------------------------

  CREATE TABLE "TOAD_PLAN_TABLE" 
   (	"STATEMENT_ID" VARCHAR2(30), 
	"PLAN_ID" NUMBER, 
	"TIMESTAMP" DATE, 
	"REMARKS" VARCHAR2(4000), 
	"OPERATION" VARCHAR2(30), 
	"OPTIONS" VARCHAR2(255), 
	"OBJECT_NODE" VARCHAR2(128), 
	"OBJECT_OWNER" VARCHAR2(30), 
	"OBJECT_NAME" VARCHAR2(30), 
	"OBJECT_ALIAS" VARCHAR2(65), 
	"OBJECT_INSTANCE" INTEGER, 
	"OBJECT_TYPE" VARCHAR2(30), 
	"OPTIMIZER" VARCHAR2(255), 
	"SEARCH_COLUMNS" NUMBER, 
	"ID" INTEGER, 
	"PARENT_ID" INTEGER, 
	"DEPTH" INTEGER, 
	"POSITION" INTEGER, 
	"COST" INTEGER, 
	"CARDINALITY" INTEGER, 
	"BYTES" INTEGER, 
	"OTHER_TAG" VARCHAR2(255), 
	"PARTITION_START" VARCHAR2(255), 
	"PARTITION_STOP" VARCHAR2(255), 
	"PARTITION_ID" INTEGER, 
	"OTHER" LONG, 
	"DISTRIBUTION" VARCHAR2(30), 
	"CPU_COST" INTEGER, 
	"IO_COST" INTEGER, 
	"TEMP_SPACE" INTEGER, 
	"ACCESS_PREDICATES" VARCHAR2(4000), 
	"FILTER_PREDICATES" VARCHAR2(4000), 
	"PROJECTION" VARCHAR2(4000), 
	"TIME" INTEGER, 
	"QBLOCK_NAME" VARCHAR2(30), 
	"OTHER_XML" CLOB
   ) ;
--------------------------------------------------------
--  DDL for Index IDX_AF_1
--------------------------------------------------------

  CREATE INDEX "IDX_AF_1" ON "ASSOCIATED_FILES" ("THIS_FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_AF_2
--------------------------------------------------------

  CREATE INDEX "IDX_AF_2" ON "ASSOCIATED_FILES" ("ASSOCATED_FILE") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_BK_1
--------------------------------------------------------

  CREATE INDEX "IDX_BK_1" ON "BLOCKS" ("DATASET_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_BP_1
--------------------------------------------------------

  CREATE INDEX "IDX_BP_1" ON "BLOCK_PARENTS" ("PARENT_BLOCK_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DC_1
--------------------------------------------------------

  CREATE INDEX "IDX_DC_1" ON "DATASET_OUTPUT_MOD_CONFIGS" ("DATASET_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DC_2
--------------------------------------------------------

  CREATE INDEX "IDX_DC_2" ON "DATASET_OUTPUT_MOD_CONFIGS" ("OUTPUT_MOD_CONFIG_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DP_1
--------------------------------------------------------

  CREATE INDEX "IDX_DP_1" ON "DATASET_PARENTS" ("PARENT_DATASET_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DR_1
--------------------------------------------------------

  CREATE INDEX "IDX_DR_1" ON "DATASET_RUNS" ("DATASET_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DR_2
--------------------------------------------------------

  CREATE INDEX "IDX_DR_2" ON "DATASET_RUNS" ("RUN_NUMBER") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DR_3
--------------------------------------------------------

  CREATE INDEX "IDX_DR_3" ON "DATASET_RUNS" ("LUMI_SECTION_COUNT") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DS_1
--------------------------------------------------------

  CREATE INDEX "IDX_DS_1" ON "DATASETS" ("PRIMARY_DS_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DS_2
--------------------------------------------------------

  CREATE INDEX "IDX_DS_2" ON "DATASETS" ("DATA_TIER_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DS_3
--------------------------------------------------------

  CREATE INDEX "IDX_DS_3" ON "DATASETS" ("PROCESSED_DS_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DS_4
--------------------------------------------------------

  CREATE INDEX "IDX_DS_4" ON "DATASETS" ("DATASET_ACCESS_TYPE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DS_5
--------------------------------------------------------

  CREATE INDEX "IDX_DS_5" ON "DATASETS" ("PHYSICS_GROUP_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DS_6
--------------------------------------------------------

  CREATE INDEX "IDX_DS_6" ON "DATASETS" ("ACQUISITION_ERA_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_DS_7
--------------------------------------------------------

  CREATE INDEX "IDX_DS_7" ON "DATASETS" ("PROCESSING_ERA_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FC_1
--------------------------------------------------------

  CREATE INDEX "IDX_FC_1" ON "FILE_OUTPUT_MOD_CONFIGS" ("FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FC_2
--------------------------------------------------------

  CREATE INDEX "IDX_FC_2" ON "FILE_OUTPUT_MOD_CONFIGS" ("OUTPUT_MOD_CONFIG_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FLM_1
--------------------------------------------------------

  CREATE INDEX "IDX_FLM_1" ON "FILE_LUMIS" ("FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_1
--------------------------------------------------------

  CREATE INDEX "IDX_FL_1" ON "FILES" ("DATASET_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_2
--------------------------------------------------------

  CREATE INDEX "IDX_FL_2" ON "FILES" ("BLOCK_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_3
--------------------------------------------------------

  CREATE INDEX "IDX_FL_3" ON "FILES" ("FILE_TYPE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_4
--------------------------------------------------------

  CREATE INDEX "IDX_FL_4" ON "FILES" ("BRANCH_HASH_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_5
--------------------------------------------------------

  CREATE INDEX "IDX_FL_5" ON "FILES" ("FILE_SIZE") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_6
--------------------------------------------------------

  CREATE INDEX "IDX_FL_6" ON "FILES" ("CREATION_DATE") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_7
--------------------------------------------------------

  CREATE INDEX "IDX_FL_7" ON "FILES" ("CREATE_BY") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FL_8
--------------------------------------------------------

  CREATE INDEX "IDX_FL_8" ON "FILES" ("IS_FILE_VALID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_FP_1
--------------------------------------------------------

  CREATE INDEX "IDX_FP_1" ON "FILE_PARENTS" ("PARENT_FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_PDS_1
--------------------------------------------------------

  CREATE INDEX "IDX_PDS_1" ON "PRIMARY_DATASETS" ("PRIMARY_DS_TYPE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index IDX_PSH_1
--------------------------------------------------------

  CREATE INDEX "IDX_PSH_1" ON "PARAMETER_SET_HASHES" ("PSET_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index ID_BK_2
--------------------------------------------------------

  CREATE INDEX "ID_BK_2" ON "BLOCKS" ("BLOCK_SIZE") 
  ;
--------------------------------------------------------
--  DDL for Index ID_BK_3
--------------------------------------------------------

  CREATE INDEX "ID_BK_3" ON "BLOCKS" ("FILE_COUNT") 
  ;
--------------------------------------------------------
--  DDL for Index ID_BK_4
--------------------------------------------------------

  CREATE INDEX "ID_BK_4" ON "BLOCKS" ("CREATION_DATE") 
  ;
--------------------------------------------------------
--  DDL for Index ID_BK_5
--------------------------------------------------------

  CREATE INDEX "ID_BK_5" ON "BLOCKS" ("CREATE_BY") 
  ;
--------------------------------------------------------
--  DDL for Index ID_DS_8
--------------------------------------------------------

  CREATE INDEX "ID_DS_8" ON "DATASETS" ("CREATION_DATE") 
  ;
--------------------------------------------------------
--  DDL for Index ID_DS_9
--------------------------------------------------------

  CREATE INDEX "ID_DS_9" ON "DATASETS" ("CREATE_BY") 
  ;
--------------------------------------------------------
--  DDL for Index ID_FLM_2
--------------------------------------------------------

  CREATE INDEX "ID_FLM_2" ON "FILE_LUMIS" ("RUN_NUM", "FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index ID_OMC_1
--------------------------------------------------------

  CREATE INDEX "ID_OMC_1" ON "OUTPUT_MODULE_CONFIGS" ("RELEASE_VERSION_ID") 
  ;
--------------------------------------------------------
--  DDL for Index ID_OMC_2
--------------------------------------------------------

  CREATE INDEX "ID_OMC_2" ON "OUTPUT_MODULE_CONFIGS" ("PARAMETER_SET_HASH_ID") 
  ;
--------------------------------------------------------
--  DDL for Index ID_OMC_3
--------------------------------------------------------

  CREATE INDEX "ID_OMC_3" ON "OUTPUT_MODULE_CONFIGS" ("OUTPUT_MODULE_LABEL") 
  ;
--------------------------------------------------------
--  DDL for Index ID_OMC_4
--------------------------------------------------------

  CREATE INDEX "ID_OMC_4" ON "OUTPUT_MODULE_CONFIGS" ("APP_EXEC_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_AE
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_AE" ON "APPLICATION_EXECUTABLES" ("APP_EXEC_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_AF
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_AF" ON "ASSOCIATED_FILES" ("ASSOCATED_FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_AQE
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_AQE" ON "ACQUISITION_ERAS" ("ACQUISITION_ERA_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_BH
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_BH" ON "BRANCH_HASHES" ("BRANCH_HASH_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_BK
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_BK" ON "BLOCKS" ("BLOCK_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_BP
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_BP" ON "BLOCK_PARENTS" ("THIS_BLOCK_ID", "PARENT_BLOCK_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DC
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DC" ON "DATASET_OUTPUT_MOD_CONFIGS" ("DS_OUTPUT_MOD_CONF_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DP
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DP" ON "DATASET_PARENTS" ("THIS_DATASET_ID", "PARENT_DATASET_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DR
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DR" ON "DATASET_RUNS" ("DATASET_RUN_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DS
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DS" ON "DATASETS" ("DATASET_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DT
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DT" ON "DATA_TIERS" ("DATA_TIER_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DTP
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DTP" ON "DATASET_ACCESS_TYPES" ("DATASET_ACCESS_TYPE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DV
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DV" ON "DBS_VERSIONS" ("DBS_VERSION_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_FC
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_FC" ON "FILE_OUTPUT_MOD_CONFIGS" ("FILE_OUTPUT_CONFIG_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_FL
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_FL" ON "FILES" ("FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_FLM
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_FLM" ON "FILE_LUMIS" ("RUN_NUM", "LUMI_SECTION_NUM", "FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_FP
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_FP" ON "FILE_PARENTS" ("THIS_FILE_ID", "PARENT_FILE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_FT
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_FT" ON "FILE_DATA_TYPES" ("FILE_TYPE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_MB
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_MB" ON "MIGRATION_BLOCKS" ("MIGRATION_BLOCK_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_MR
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_MR" ON "MIGRATION_REQUESTS" ("MIGRATION_REQUEST_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_OMC
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_OMC" ON "OUTPUT_MODULE_CONFIGS" ("OUTPUT_MOD_CONFIG_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PDS
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PDS" ON "PRIMARY_DATASETS" ("PRIMARY_DS_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PDT
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PDT" ON "PRIMARY_DS_TYPES" ("PRIMARY_DS_TYPE_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PE
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PE" ON "PROCESSING_ERAS" ("PROCESSING_ERA_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PG
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PG" ON "PHYSICS_GROUPS" ("PHYSICS_GROUP_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PSDS
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PSDS" ON "PROCESSED_DATASETS" ("PROCESSED_DS_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PSH
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PSH" ON "PARAMETER_SET_HASHES" ("PARAMETER_SET_HASH_ID") 
  ;
--------------------------------------------------------
--  DDL for Index PK_RV
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_RV" ON "RELEASE_VERSIONS" ("RELEASE_VERSION_ID") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_AE_APP_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_AE_APP_NAME" ON "APPLICATION_EXECUTABLES" ("APP_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_AF_1
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_AF_1" ON "ASSOCIATED_FILES" ("THIS_FILE_ID", "ASSOCATED_FILE") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_AQE_ACQUISITION_ERA_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_AQE_ACQUISITION_ERA_NAME" ON "ACQUISITION_ERAS" ("ACQUISITION_ERA_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_AQE_ACQUISITION_ERA_NAME2
--------------------------------------------------------

--  CREATE UNIQUE INDEX "TUC_AQE_ACQUISITION_ERA_NAME2" ON "ACQUISITION_ERAS" (NLSSORT("ACQUISITION_ERA_NAME",'nls_sort=''BINARY_CI''')) 
  ;
--------------------------------------------------------
--  DDL for Index TUC_BK_BLOCK_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_BK_BLOCK_NAME" ON "BLOCKS" ("BLOCK_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_DC_1
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_DC_1" ON "DATASET_OUTPUT_MOD_CONFIGS" ("DATASET_ID", "OUTPUT_MOD_CONFIG_ID") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_DS_DATASET
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_DS_DATASET" ON "DATASETS" ("DATASET") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_DTP_DATASET_ACCESS_TYPE
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_DTP_DATASET_ACCESS_TYPE" ON "DATASET_ACCESS_TYPES" ("DATASET_ACCESS_TYPE") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_DT_DATA_TIER_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_DT_DATA_TIER_NAME" ON "DATA_TIERS" ("DATA_TIER_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_FC_1
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_FC_1" ON "FILE_OUTPUT_MOD_CONFIGS" ("FILE_ID", "OUTPUT_MOD_CONFIG_ID") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_FL_LOGICAL_FILE_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_FL_LOGICAL_FILE_NAME" ON "FILES" ("LOGICAL_FILE_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_FT_FILE_TYPE
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_FT_FILE_TYPE" ON "FILE_DATA_TYPES" ("FILE_TYPE") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_MB_1
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_MB_1" ON "MIGRATION_BLOCKS" ("MIGRATION_BLOCK_NAME", "MIGRATION_REQUEST_ID") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_MR_1
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_MR_1" ON "MIGRATION_REQUESTS" ("MIGRATION_INPUT") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_OMC_1
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_OMC_1" ON "OUTPUT_MODULE_CONFIGS" ("APP_EXEC_ID", "RELEASE_VERSION_ID", "PARAMETER_SET_HASH_ID", "OUTPUT_MODULE_LABEL", "GLOBAL_TAG") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_PDS_PRIMARY_DS_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_PDS_PRIMARY_DS_NAME" ON "PRIMARY_DATASETS" ("PRIMARY_DS_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_PDT_PRIMARY_DS_TYPE
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_PDT_PRIMARY_DS_TYPE" ON "PRIMARY_DS_TYPES" ("PRIMARY_DS_TYPE") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_PE_PROCESSING_VERSION
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_PE_PROCESSING_VERSION" ON "PROCESSING_ERAS" ("PROCESSING_VERSION") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_PG_PHYSICS_GROUP_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_PG_PHYSICS_GROUP_NAME" ON "PHYSICS_GROUPS" ("PHYSICS_GROUP_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_PSDS_PROCESSED_DS_NAME
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_PSDS_PROCESSED_DS_NAME" ON "PROCESSED_DATASETS" ("PROCESSED_DS_NAME") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_PSH_PSET_HASH
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_PSH_PSET_HASH" ON "PARAMETER_SET_HASHES" ("PSET_HASH") 
  ;
--------------------------------------------------------
--  DDL for Index TUC_RV_RELEASE_VERSION
--------------------------------------------------------

  CREATE UNIQUE INDEX "TUC_RV_RELEASE_VERSION" ON "RELEASE_VERSIONS" ("RELEASE_VERSION") 
  ;
