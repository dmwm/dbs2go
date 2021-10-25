SELECT t.owner AS owner, t.table_name AS table_name, 
    t.index_name AS index_name, s.bytes AS table_index_size
FROM dba_segments s, dba_indexes t
WHERE s.owner=t.owner AND  s.segment_name=t.index_name 
    AND s.owner LIKE '{{.Owner}}%' AND s.segment_type='INDEX' ORDER BY 1, 4
