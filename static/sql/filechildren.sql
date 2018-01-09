SELECT CF.LOGICAL_FILE_NAME child_logical_file_name,
         CF.FILE_ID child_file_id,
         F.LOGICAL_FILE_NAME
         FROM {{.Owner}}.FILES CF
         JOIN {{.Owner}}.FILE_PARENTS FP ON FP.THIS_FILE_ID = CF.FILE_ID
         JOIN {{.Owner}}.FILES F ON  F.FILE_ID = FP.PARENT_FILE_ID
