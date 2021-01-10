{{if .Query1}}
insert into {{.Owner}}.block_parents (this_block_id, parent_block_id)
       values (:this_block_id,
          (select block_id as parent_block_id from {{.Owner}}.blocks where block_name=:block_name) )
{{end}}
{{if .Query2}}
insert into {{.Owner}}.block_parents (this_block_id, parent_block_id)
       values(:this_block_id,
          (select block_id as parent_block_id from {{.Owner}}.files where logical_file_name=:parent_logical_file_name))
{{end}}
{{if .Query2}}
insert into {{.Owner}}.block_parents (this_block_id, parent_block_id)
       values((select block_id as this_block_id from {{.Owner}}.blocks where block_name=:block_name),:parent_block_id )
{{end}}
