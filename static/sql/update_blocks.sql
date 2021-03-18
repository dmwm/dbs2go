{{if .Site}}
UPDATE {{.Owner}}.BLOCKS
    SET ORIGIN_SITE_NAME = :origin_site_name ,
        LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE = :mtime
    WHERE BLOCK_NAME = :block_name
{{else}}
UPDATE {{.Owner}}.BLOCKS
    SET OPEN_FOR_WRITING = :open_for_writing ,
        LAST_MODIFIED_BY=:myuser,
        LAST_MODIFICATION_DATE = :ltime
    WHERE BLOCK_NAME = :block_name
{{end}}
