--INSERT INTO APPLICATION_EXECUTABLES
--    (app_exec_id, app_name)
--    VALUES (:app_exec_id:, :app_name:);
--INSERT INTO RELEASE_VERSIONS
--    (release_version_id, release_version)
--    VALUES (:release_version_id:, :release_version:);
--INSERT INTO PARAMETER_SET_HASHES
--    (parameter_set_hash_id, pset_hash)
--    VALUES (:parameter_set_hash_id:, :pset_hash:);
INSERT INTO OUTPUT_MODULE_CONFIGS
    ( output_mod_config_id, app_exec_id, release_version_id,
      parameter_set_hash_id, output_module_label, global_tag,
      scenario, creation_date, create_by )
    VALUES ( :output_mod_config_id:, :app_exec_id:, :release_version_id:,
      :parameter_set_hash_id:, :output_module_label:, :global_tag:,
      :scenario:, :creation_date:, :create_by: )
