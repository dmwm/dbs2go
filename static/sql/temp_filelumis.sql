CREATE PRIVATE TEMPORARY TABLE {{.TempTable}}
(RUN_NUM INTEGER, LUMI_SECTION_NUM INTEGER, FILE_ID INTEGER, EVENT_COUNT INTEGER)
ON COMMIT DROP DEFINITION
