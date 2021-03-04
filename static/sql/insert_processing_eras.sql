INSERT INTO {{.Owner}}.PROCESSING_ERAS
    (processing_era_id,processing_version,creation_date,create_by,description)
    VALUES
    (:processing_era_id,:processing_version,:creation_date,:create_by,:description)
