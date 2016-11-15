SELECT (
    SELECT NVL(SUM(BS.BLOCK_SIZE), 0)
    FROM {{.Owner}}.BLOCKS BS dataset_join where_clause
) AS FILE_SIZE,
(
    SELECT NVL(SUM(BS.FILE_COUNT),0)
    FROM {{.Owner}}.BLOCKS BS dataset_join where_clause
) AS NUM_FILE,
(
    SELECT NVL(SUM(FS.EVENT_COUNT),0)
    FROM {{.Owner}}.FILES FS block_join dataset_join where_clause
) AS NUM_EVENT
FROM DUAL
