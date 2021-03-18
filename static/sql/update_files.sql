UPDATE {{.Owner}}.FILES F
    SET LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE=:mydate,
        IS_FILE_VALID = :is_file_valid
