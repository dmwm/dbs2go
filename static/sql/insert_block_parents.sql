{{if .Query1}}
insert into {{.Owner}}.block_parents (this_block_id, parent_block_id)
       values (?:ThisBlockID,
          (select block_id as parent_block_id from {{.Owner}}.blocks where block_name=?:BlockName) )
{{end}}
{{if .Query2}}
insert into {{.Owner}}.block_parents (this_block_id, parent_block_id)
       values(?:ThisBlockID,
          (select block_id as parent_block_id from {{.Owner}}.files where logical_file_name=?:ParentLogicalFileName))
{{end}}
{{if .Query2}}
insert into {{.Owner}}.block_parents (this_block_id, parent_block_id)
       values((select block_id as this_block_id from {{.Owner}}.blocks where block_name=?:BlockName),?:ParentBlockID )
{{end}}