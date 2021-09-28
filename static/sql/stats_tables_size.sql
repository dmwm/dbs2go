SELECT t.owner AS owner, t.table_name AS table_name, 
    t.num_rows AS nrows, s.bytes AS table_size
FROM dba_segments s, dba_tables t
WHERE s.owner=t.owner AND s.segment_name=t.table_name 
    AND s.owner LIKE '{{.Owner}}%' AND s.segment_type='TABLE' ORDER BY 1, 3;
