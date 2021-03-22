INSERT INTO {{.Owner}}.file_lumis
    (run_num, lumi_section_num, file_id, event_count)
    VALUES (:run_num, :lumi_section_num, :file_id, :event_count)
