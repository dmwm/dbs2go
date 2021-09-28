SELECT owner, sum(bytes) AS schema_size
FROM dba_segments
WHERE owner LIKE '{{.Owner}}%' GROUP BY owner;
