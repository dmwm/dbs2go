select t.owner, t.table_name,t.index_name, s.bytes/1024/1024 as size_MB from dba_segments s,
dba_indexes t where s.owner=t.owner and  s.segment_name=t.index_name and s.owner like 'CMS_DBS3%' and
s.segment_type='INDEX' order by 1, 4;
