INSERT ALL
WHEN not exists(
	select app_exec_id from ((.Owner)).application_executables where app_name = app_n)
THEN
     INTO ((.Owner)).application_executables(app_exec_id, app_name)values(((.Owner)).seq_ae.nextval, app_n)
WHEN not exists (
	select release_version_id from ((.Owner)).release_versions where release_version = release_v)
THEN
     INTO ((.Owner)).release_versions(release_version_id, release_version) values (((.Owner)).seq_rv.nextval, release_v)
WHEN not exists(
	select parameter_set_hash_id from ((.Owner)).parameter_set_hashes where pset_hash = pset_h)
THEN
     INTO ((.Owner)).parameter_set_hashes ( parameter_set_hash_id, pset_hash, pset_name ) values (((.Owner)).seq_psh.nextval, pset_h, pset_name)
WHEN 1=1 THEN
     INTO ((.Owner)).output_module_configs ( output_mod_config_id, app_exec_id, release_version_id,
     parameter_set_hash_id, output_module_label, global_tag, scenario, creation_date, create_by
     ) values (((.Owner)).seq_omc.nextval,
     NVL((select app_exec_id from ((.Owner)).application_executables where app_name = app_n),((.Owner)).seq_ae.nextval),
     NVL((select release_version_id from ((.Owner)).release_versions where release_version = release_v), ((.Owner)).seq_rv.nextval),
     NVL((select parameter_set_hash_id from  ((.Owner)).parameter_set_hashes where pset_hash = pset_h), ((.Owner)).seq_psh.nextval),
     :output_module_label, :global_tag, :scenario, :creation_date, :create_by)
select :app_name app_n, :release_version release_v, :pset_hash pset_h, :pset_name pset_name from dual
