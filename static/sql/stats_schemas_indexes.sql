SELECT owner, sum(bytes) AS schema_index_size
FROM dba_segments
WHERE owner LIKE '{{.Owner}}%' AND segment_type='INDEX' GROUP BY owner;
