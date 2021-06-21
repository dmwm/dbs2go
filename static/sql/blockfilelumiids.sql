SELECT  run_num AS R, Lumi_section_num AS L, file_id AS cfid FROM {{.Owner}}.file_lumis fl
        WHERE fl.file_id IN (SELECT file_id FROM {{.Owner}}.files f
        inner join {{.Owner}}.blocks b on f.block_id = b.block_id
{{if .ChildLfnList}}
        WHERE b.block_name = :block_name
        AND f.logical_file_name IN (SELECT TOKEN FROM TOKEN_GENERATOR) ) ORDER BY cfid
{{else}}
        WHERE b.block_name = :block_name )
{{end}}
