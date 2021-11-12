INSERT
{{if eq .Owner "sqlite"}}
OR IGNORE 
{{else}}
/*+ ignore_row_on_dupkey_index ( FL ( run_num,lumi_section_num,file_id ) ) */
{{end}}
INTO {{.Owner}}.file_lumis
(run_num, lumi_section_num, file_id, event_count)
VALUES (:run_num, :lumi_section_num, :file_id, :event_count)
