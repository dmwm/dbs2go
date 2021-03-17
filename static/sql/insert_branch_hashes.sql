INSERT INTO {{.Owner}}.BRANCH_HASHES
    (branch_hash_id,branch_hash,content)
    VALUES
    (:branch_hash_id,:branch_hash,:content)
