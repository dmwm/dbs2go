INSERT INTO {{.DBOwner}}.FILE_OUTPUT_MOD_CONFIGS
    (file_output_config_id,file_id,output_mod_config_id)
    VALUES
    (:file_output_config_id,:file_id,:output_mod_config_id)
