 select
(select count(f.file_id)  from {{.Owner}}.files f
  join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id
{{if .Valid}}
  JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
 JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
{{end}}
  where b.BLOCK_NAME=:block_name wheresql_isFileValid
  and f.FILE_ID in (select fl.file_id from {{.Owner}}.file_lumis fl where whererun )
 ) as num_file,
 nvl((select sum(f.event_count) event_count from {{.Owner}}.files f
  join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id
{{if .Valid}}
  JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
{{end}}
  where b.BLOCK_NAME=:block_name wheresql_isFileValid and
  f.FILE_ID in (select fl.file_id from {{.Owner}}.file_lumis fl where whererun)
 ),0) as num_event,
 (select nvl(sum(f.file_size),0) file_size from {{.Owner}}.files f
  join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id
{{if .Valid}}
  JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
{{end}}
  where b.BLOCK_NAME=:block_name wheresql_isFileValid and
  f.FILE_ID in (select fl.file_id from {{.Owner}}.file_lumis fl where whererun)
 ) as file_size,
(select count(distinct b.block_id) from {{.Owner}}.blocks b
  join {{.Owner}}.files f on f.block_id=b.block_id
{{if .Valid}}
  JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
{{end}}
where b.block_name=:block_name wheresql_isFileValid and
f.FILE_ID in (select fl.file_id from {{.Owner}}.file_lumis fl where whererun)
)as num_block,
(select count(*) from (select distinct fl.lumi_section_num, fl.run_num from {{.Owner}}.files f
 join {{.Owner}}.file_lumis fl on fl.file_id=f.file_id
 join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id
{{if .Valid}}
 JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN {{.Owner}}.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID
{{end}}
 where b.BLOCK_NAME=:block_name wheresql_isFileValid and whererun )
) as num_lumi
from dual
