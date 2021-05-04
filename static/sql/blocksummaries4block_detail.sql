{{if eq .Owner "sqlite"}}
select
    b.block_name as block_name,
    b.file_count as num_file,
    b.block_size as file_size,
    t1.num_event as num_event,
    b.open_for_writing as open_for_writing
from
    blocks b,
    (
        select bs.block_name as block_name,
        sum(fs.event_count) as num_event
        FROM FILES FS
        JOIN BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
        WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
        group by bs.block_name
    ) t1
where
    t1.block_name = b.block_name
{{else}}
select
    b.block_name as block_name,
    b.file_count as num_file,
    b.block_size as file_size,
    t1.num_event as num_event,
    b.open_for_writing as open_for_writing
from
    {{.Owner}}.blocks b,
    (
        select bs.block_name as block_name,
        NVL(sum(fs.event_count),0) as num_event
        FROM {{.Owner}}.FILES FS
        JOIN {{.Owner}}.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID
        WHERE BS.BLOCK_NAME IN {{.TokenCondition}}
        group by bs.block_name ) t1
where
    t1.block_name = b.block_name
{{end}}
