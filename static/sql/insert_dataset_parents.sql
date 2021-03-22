INSERT INTO {{.Owner}}.DATASET_PARENTS
    ( this_dataset_id, parent_dataset_id)
    VALUES (:this_dataset_id, :parent_dataset_id)
