INSERT INTO {{.Owner}}.DATA_TIERS
    (data_tier_id, data_tier_name, creation_date, create_by)
    VALUES
    (:data_tier_id, :data_tier_name, :creation_date, :create_by)
