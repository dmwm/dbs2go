SELECT
    O.OUTPUT_MOD_CONFIG_ID
FROM {{.Owner}}.OUTPUT_MODULE_CONFIGS O
INNER JOIN {{.Owner}}.RELEASE_VERSIONS R ON O.RELEASE_VERSION_ID=R.RELEASE_VERSION_ID
INNER JOIN {{.Owner}}.APPLICATION_EXECUTABLES A ON O.APP_EXEC_ID=A.APP_EXEC_ID
INNER JOIN {{.Owner}}.PARAMETER_SET_HASHES P ON O.PARAMETER_SET_HASH_ID=P.PARAMETER_SET_HASH_ID
WHERE A.APP_NAME = :app_name
AND P.PSET_HASH=:pset_hash
AND R.RELEASE_VERSION=:release_version
AND O.OUTPUT_MODULE_LABEL=:output_module_label
AND O.GLOBAL_TAG =:global_tag