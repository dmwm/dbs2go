INSERT INTO {{.Owner}}.BLOCK_PARENTS 
    (this_block_id, parent_block_id)
    VALUES (:this_block_id,:parent_block_id)
