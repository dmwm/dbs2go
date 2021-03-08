INSERT INTO {{.Owner}}.FILE_PARENTS
    (this_file_id, parent_file_id)
    VALUES
    (:this_file_id, :parent_logical_file_name)
