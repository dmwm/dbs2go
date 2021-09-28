SELECT sum(bytes) AS db_index_size
FROM dba_segments WHERE
owner LIKE '{{.Owner}}%' and segment_type='INDEX';
