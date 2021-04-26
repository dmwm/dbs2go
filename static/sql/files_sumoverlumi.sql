with myfiles as (
    {{.Statement}}
) select mf.* ,
            (case
                when badi.file_id = mc.file_id and badi.run_num=mc.run_num and badi.bid is null then null
                else  mc.event_count
             end) as event_count
     from myfiles mf,
{{if .LumiList}}
{{.LumiGenerator}}
(select sum(fl.event_count) as event_count, fl.file_id, fl.run_num
                            from {{.Owner}}.file_lumis fl
                            join myfiles mf on mf.file_id=fl.file_id and mf.run_num=fl.run_num
                            where sql_lumi
                            group by fl.file_id, fl.run_num) mc,
{{else}}
(select sum(fl.event_count) as event_count, fl.file_id, fl.run_num
                            from {{.Owner}}.file_lumis fl
                            join myfiles mf on mf.file_id=fl.file_id and mf.run_num=fl.run_num
                            group by fl.file_id, fl.run_num) mc,
{{end}}
          (
            select distinct fl.file_id, fl.run_num, null as bid
            from {{.Owner}}.file_lumis fl
            join myfiles my2 on my2.file_id=fl.file_id and my2.run_num=fl.run_num
            where fl.event_count is null
         )badi
where mf.file_id= mc.file_id and mf.run_num=mc.run_num
