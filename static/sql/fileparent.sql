SELECT F.LOGICAL_FILE_NAME this_logical_file_name, PF.LOGICAL_FILE_NAME parent_logical_file_name,
        PF.FILE_ID parent_file_id
        FROM {{.Owner}}.FILES PF
        JOIN {{.Owner}}.FILE_PARENTS FP ON FP.PARENT_FILE_ID = PF.FILE_ID
        JOIN {{.Owner}}.FILES F ON  F.FILE_ID = FP.THIS_FILE_ID
