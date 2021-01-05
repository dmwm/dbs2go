select
    b.block_name as block_name,
    b.file_count as num_file,
    b.block_size as file_size,
    t1.num_event as num_event,
    b.open_for_writing as open_for_writing
from
    {{.Owner}}.blocks b,
    (select
        bs.block_name as block_name,
        NVL(sum(fs.event_count),0) as num_event
    from
        {{.Owner}}.files fs
    {{.BlockJoin}}
    {{.WhereClause}}
    group by bs.block_name )t1
where
    t1.block_name = b.block_name
