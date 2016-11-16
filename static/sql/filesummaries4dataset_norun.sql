select
(select count(f.file_id)  from {{.Owner}}.files f
  join {{.Owner}}.datasets d on d.DATASET_ID = f.dataset_id join_valid_ds2
  where d.dataset=:dataset wheresql_isFileValid
 ) as num_file,

 nvl((select sum(f.event_count) event_count from {{.Owner}}.files f
  join {{.Owner}}.datasets d on d.DATASET_ID = f.dataset_id join_valid_ds2
  where d.dataset=:dataset wheresql_isFileValid
 ),0) as num_event,

 (select nvl(sum(f.file_size),0) file_size from {{.Owner}}.files f
  join {{.Owner}}.datasets d on d.DATASET_ID = f.dataset_id join_valid_ds2
  where d.dataset=:dataset wheresql_isFileValid
 ) as file_size,

 (select count(b.block_id) from {{.Owner}}.blocks b
  join {{.Owner}}.datasets d on d.dataset_id = b.dataset_id join_valid_ds2
  where d.dataset=:dataset
 ) as num_block,

(select count(*) from (select distinct l.lumi_section_num, l.run_num from {{.Owner}}.files f
 join {{.Owner}}.file_lumis l on l.file_id=f.file_id
 join {{.Owner}}.datasets d on d.DATASET_ID = f.dataset_id join_valid_ds2
 where d.dataset=:dataset wheresql_isFileValid)
) as num_lumi
 from dual
