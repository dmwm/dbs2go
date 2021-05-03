{{if .ChildLfnList }}

{{.LfnTokenGenerator}}
with parents as (
select run_num as R, Lumi_section_num as L, file_id as pid from {{.Owner}}.file_lumis fl
where fl.file_id in (
	select file_id from {{.Owner}}.files f
	where F.DATASET_ID in (
		select parent_dataset_id from {{.Owner}}.dataset_parents dp
		inner join {{.Owner}}.datasets d on d.dataset_id=DP.THIS_DATASET_ID
		where d.dataset = :child_ds_name )
)
),
children as (
select  run_num as R, Lumi_section_num as L, file_id as cid from {{.Owner}}.file_lumis fl
where fl.file_id in (
	select file_id from {{.Owner}}.files f
	inner join {{.Owner}}.blocks b on f.block_id = b.block_id
	where b.block_name = :child_block_name 
		and f.logical_file_name in {{.TokenCondition}}
)
select distinct cid, pid from children c inner join parents p on c.R = p.R and c.L = p.L

{{else}}

with parents as (
select run_num as R, Lumi_section_num as L, file_id as pid from {{.Owner}}.file_lumis fl
where fl.file_id in (
	select file_id from {{.Owner}}.files f
	where F.DATASET_ID in (
		select parent_dataset_id from {{.Owner}}.dataset_parents dp
		inner join {{.Owner}}.datasets d on d.dataset_id=DP.THIS_DATASET_ID
		where d.dataset = :child_ds_name )
)
),
children as (
select  run_num as R, Lumi_section_num as L, file_id as cid from {{.Owner}}.file_lumis fl
where fl.file_id in (
	select file_id from {{.Owner}}.files f
	inner join {{.Owner}}.blocks b on f.block_id = b.block_id
	where b.block_name = :child_block_name )
)
select distinct cid, pid from children c inner join parents p on c.R = p.R and c.L = p.L
{{end}}
