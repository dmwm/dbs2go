WITH myfiles AS (
    {{.Statement}}
) select mf.* ,
            (case
                when badi.file_id = mc.file_id and badi.run_num=mc.run_num and badi.bid is null then null
                else  mc.event_count
             end) as event_count
     FROM myfiles mf,
{{if .LumiList}}
{{.LumiGenerator}}
          (
            SELECT sum(fl.event_count) as event_count, fl.file_id, fl.run_num
            FROM {{.Owner}}.file_lumis fl
            JOIN myfiles mf on mf.file_id=fl.file_id and mf.run_num=fl.run_num
            WHERE sql_lumi
            GROUP BY fl.file_id, fl.run_num
          ) mc,
{{else}}
          (
            SELECT sum(fl.event_count) as event_count, fl.file_id, fl.run_num
            FROM {{.Owner}}.file_lumis fl
            JOIN myfiles mf on mf.file_id=fl.file_id and mf.run_num=fl.run_num
            GROUP BY fl.file_id, fl.run_num
          ) mc,
{{end}}
          (
            SELECT distinct fl.file_id, fl.run_num, null as bid
            FROM {{.Owner}}.file_lumis fl
            JOIN myfiles my2 on my2.file_id=fl.file_id and my2.run_num=fl.run_num
            WHERE fl.event_count is null
          ) badi
     WHERE mf.file_id= mc.file_id and mf.run_num=mc.run_num
