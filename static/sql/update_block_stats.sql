UPDATE {{.Owner}}.BLOCKS
SET FILE_COUNT=:file_count, BLOCK_SIZE=:block_size
WHERE BLOCK_ID=:block_id
