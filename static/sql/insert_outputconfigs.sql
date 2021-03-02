INSERT ALL
WHEN not exists (
	SELECT app_exec_id
    FROM {{.Owner}}.application_executables
    WHERE app_name = app_n
    )
THEN
    INTO {{.Owner}}.application_executables
    (app_exec_id, app_name)
    VALUES ({{.Owner}}.seq_ae.nextval, app_n)
WHEN not exists (
	SELECT release_version_id
    FROM {{.Owner}}.release_versions
    WHERE release_version = release_v
    )
THEN
    INTO {{.Owner}}.release_versions
    (release_version_id, release_version)
    VALUES ({{.Owner}}.seq_rv.nextval, release_v)
WHEN not exists (
	SELECT parameter_set_hash_id
    FROM {{.Owner}}.parameter_set_hashes
    WHERE pset_hash = pset_h
    )
THEN
    INTO {{.Owner}}.parameter_set_hashes
    ( parameter_set_hash_id, pset_hash, pset_name )
    VALUES ({{.Owner}}.seq_psh.nextval, pset_h, pset_name)
WHEN 1=1 THEN
    INTO {{.Owner}}.output_module_configs
    ( output_mod_config_id, app_exec_id, release_version_id,
      parameter_set_hash_id, output_module_label, global_tag,
      scenario, creation_date, create_by
    )
    VALUES ({{.Owner}}.seq_omc.nextval,
    NVL( ( SELECT app_exec_id
           FROM {{.Owner}}.application_executables
           WHERE app_name = app_n
         ), {{.Owner}}.seq_ae.nextval
    ),
    NVL( ( SELECT release_version_id
           FROM {{.Owner}}.release_versions
           WHERE release_version = release_v
         ), {{.Owner}}.seq_rv.nextval
    ),
    NVL( ( SELECT parameter_set_hash_id
           FROM  {{.Owner}}.parameter_set_hashes
           WHERE pset_hash = pset_h
         ), {{.Owner}}.seq_psh.nextval
    ),
    :output_module_label, :global_tag, :scenario, :creation_date, :create_by)
SELECT :app_name app_n, :release_version release_v, :pset_hash pset_h, :pset_name pset_name
    FROM dual
