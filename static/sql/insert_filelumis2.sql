INSERT 
{{if eq .Owner "sqlite"}}
OR IGNORE 
INTO {{.Owner}}.file_lumis
{{else}}
/*+ ignore_row_on_dupkey_index ( FL ( run_num,lumi_section_num,file_id ) ) */
INTO {{.Owner}}.file_lumis FL
{{end}}
(run_num, lumi_section_num, file_id)
VALUES (:run_num, :lumi_section_num, :file_id)
