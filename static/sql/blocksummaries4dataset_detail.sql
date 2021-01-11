{{if .Dataset}}
{{if .Detail}}
with t1 as(
     SELECT
         BS.BLOCK_NAME as BLOCK_NAME,
         NVL(SUM(FS.EVENT_COUNT),0) as NUM_EVENT
     FROM
         {(.Owner}}.FILES FS
     JOIN {{.Owner}}.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
     JOIN {{.Owner}}.DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID
     WHERE DS.dataset=?
     group by BS.BLOCK_NAME
 )
 select
     b.block_name as block_name,
     b.file_count as num_file,
     b.block_size as file_size,
     t1.num_event as num_event,
     b.open_for_writing as open_for_writing
from
     {{.Owner}}.blocks b, t1
where
     t1.block_name = b.block_name
{{else}}
SELECT (
        SELECT NVL(SUM(BS.BLOCK_SIZE), 0)
        FROM {{.Owner}}.BLOCKS BS
        JOIN {{.Owner}}.DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID
        WHERE DS.dataset=?
     ) AS FILE_SIZE,
     (
        SELECT NVL(SUM(BS.FILE_COUNT),0)
        FROM {{.Owner}}. BLOCKS BS
        JOIN {{.Owner}}.DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID
        WHERE DS.dataset=?
     ) AS NUM_FILE,
     (
        SELECT NVL(SUM(FS.EVENT_COUNT),0)
        FROM {{.Owner}}.FILES FS
		JOIN {{.Owner}}.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
        JOIN {{.Owner}}.DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID
        WHERE DS.dataset=?
     ) AS NUM_EVENT
     FROM DUAL
{{end}}

{{else}}
{{if .Detail}}
select
    b.block_name as block_name,
    b.file_count as num_file,
    b.block_size as file_size,
    t1.num_event as num_event,
    b.open_for_writing as open_for_writing
from
    {{.Owner}}.blocks b,
    (select
        bs.block_name as block_name,
        NVL(sum(fs.event_count),0) as num_event
    from
        {{.Owner}}.files fs
	JOIN {{.Owner}}.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
	WHERE BS.BLOCK_NAME IN (SELECT TOKEN FROM TOKEN_GENERATOR)
    group by bs.block_name )t1
where
    t1.block_name = b.block_name
{{else}}
SELECT (
        SELECT NVL(SUM(BS.BLOCK_SIZE),0)
        FROM {{.Owner}}.BLOCKS BS
		WHERE BS.BLOCK_NAME IN (SELECT TOKEN FROM TOKEN_GENERATOR)
        ) AS FILE_SIZE,
        (
        SELECT NVL(SUM(BS.FILE_COUNT),0)
        FROM {{.Owner}}.BLOCKS BS
		WHERE BS.BLOCK_NAME IN (SELECT TOKEN FROM TOKEN_GENERATOR)
        ) AS NUM_FILE,
        (
        SELECT NVL(SUM(FS.EVENT_COUNT),0)
        FROM {{.Owner}}.FILES FS
		JOIN {{.Owner}}.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
		WHERE BS.BLOCK_NAME IN (SELECT TOKEN FROM TOKEN_GENERATOR)
        ) AS NUM_EVENT
        FROM DUAL
{{end}}
{{end}}
