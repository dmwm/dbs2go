SELECT sum(bytes) AS db_size
FROM dba_segments WHERE owner LIKE '{{.Owner}}%'
