insert into {{.Owner}}.block_parents (this_block_id, parent_block_id)
      values (?:ThisBlockID,
          (select block_id as parent_block_id from {{.Owner}}.blocks where block_name=?:BlockName) )
