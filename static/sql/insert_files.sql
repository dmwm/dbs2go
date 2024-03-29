INSERT INTO {{.Owner}}.FILES
    (file_id,logical_file_name,is_file_valid,
     dataset_id,block_id,file_type_id,
     check_sum,file_size,event_count,
     adler32,md5,
     auto_cross_section,creation_date,create_by,
     last_modification_date,last_modified_by)
    VALUES
    (:file_id,:logical_file_name,:is_file_valid,
     :dataset_id,:block_id,:file_type_id,
     :check_sum,:file_size,:event_count,:adler32,:md5,
     :auto_cross_section,:creation_date,:create_by,
     :last_modification_date,:last_modified_by)
