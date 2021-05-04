{{if eq .Owner "sqlite"}}
SELECT * FROM
    (
        SELECT COALESCE(SUM(BS.BLOCK_SIZE), 0) as FILE_SIZE
        FROM BLOCKS BS
        WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
    ),
    (
        SELECT COALESCE(SUM(BS.FILE_COUNT), 0) AS NUM_FILE
        FROM BLOCKS BS
        WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
    ),
    (
        SELECT COALESCE(SUM(FS.EVENT_COUNT), 0) AS NUM_EVENT
        FROM FILES FS
        JOIN BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
        WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
    )
{{else}}
SELECT (
    SELECT NVL(SUM(BS.BLOCK_SIZE),0)
    FROM {{.Owner}}.BLOCKS BS
    WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
) AS FILE_SIZE,
(
    SELECT NVL(SUM(BS.FILE_COUNT),0)
    FROM {{.Owner}}.BLOCKS BS
    WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
) AS NUM_FILE,
(
    SELECT NVL(SUM(FS.EVENT_COUNT),0)
    FROM {{.Owner}}.FILES FS
    JOIN {{.Owner}}.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
    WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
) AS NUM_EVENT
FROM DUAL
{{end}}
