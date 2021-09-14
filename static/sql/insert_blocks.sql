INSERT INTO {{.Owner}}.BLOCKS
    (block_id,block_name,dataset_id,
     open_for_writing,origin_site_name,
     block_size,file_count,
     creation_date,create_by,
     last_modification_date,last_modified_by)
    VALUES
    (:block_id,:block_name,:dataset_id,
     :open_for_writing,:origin_site_name,
     :block_size,:file_count,
     :creation_date,:create_by,
     :last_modification_date,:last_modified_by)
