{{if eq .Owner "sqlite"}}
with t1 as(
     SELECT
         BS.BLOCK_NAME as BLOCK_NAME,
         SUM(FS.EVENT_COUNT) as NUM_EVENT
     FROM FILES FS
     JOIN BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
     JOIN DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID
     WHERE DS.dataset=:dataset
     group by BS.BLOCK_NAME
 )
 select
     b.block_name as block_name,
     b.file_count as num_file,
     b.block_size as file_size,
     t1.num_event as num_event,
     b.open_for_writing as open_for_writing
from
     blocks b, t1
where
     t1.block_name = b.block_name
{{else}}
with t1 as(
     SELECT
         BS.BLOCK_NAME as BLOCK_NAME,
         NVL(SUM(FS.EVENT_COUNT),0) as NUM_EVENT
     FROM {{.Owner}}.FILES FS
     JOIN {{.Owner}}.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
     JOIN {{.Owner}}.DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID
     WHERE DS.dataset=:dataset
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
{{end}}
