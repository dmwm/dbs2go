INSERT INTO {{.Owner}}.DATASET_OUTPUT_MOD_CONFIGS
    (ds_output_mod_conf_id,dataset_id,output_mod_config_id)
    VALUES
    (:ds_output_mod_conf_id,:dataset_id,:output_mod_config_id)
