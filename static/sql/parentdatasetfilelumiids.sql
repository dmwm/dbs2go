SELECT run_num AS R, Lumi_section_num AS L, file_id AS pfid FROM {{.Owner}}.file_lumis fl
        WHERE fl.file_id IN (SELECT file_id FROM {{.Owner}}.files f
        WHERE F.DATASET_ID IN (SELECT parent_dataset_id FROM {{.Owner}}.dataset_parents dp
        INNER JOIN {{.Owner}}.datasets d on d.dataset_id=DP.THIS_DATASET_ID
        WHERE d.dataset = :dataset )) ORDER BY pfid
