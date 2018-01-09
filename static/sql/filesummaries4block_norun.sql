select
(select count(f.file_id)  from {{.Owner}}.files f
  join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id join_valid_ds1
  where b.BLOCK_NAME=:block_name wheresql_isFileValid
 ) as num_file,

 nvl((select sum(f.event_count) event_count from {{.Owner}}.files f
  join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id join_valid_ds1
  where b.BLOCK_NAME=:block_name wheresql_isFileValid
 ),0) as num_event,

 (select nvl(sum(f.file_size),0) file_size from {{.Owner}}.files f
  join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id join_valid_ds1
  where b.BLOCK_NAME=:block_name wheresql_isFileValid
 ) as file_size,

 (select count(block_id) from {{.Owner}}.blocks where block_name=:block_name
 ) as num_block,

(select count(*) from (select distinct l.lumi_section_num, l.run_num from {{.Owner}}.files f
 join {{.Owner}}.file_lumis l on l.file_id=f.file_id
 join {{.Owner}}.blocks b on b.BLOCK_ID = f.block_id join_valid_ds1
 where b.BLOCK_NAME=:block_name wheresql_isFileValid)
) as num_lumi
from dual
