SELECT PD.DATASET parent_dataset,
       PD.DATASET_ID parent_dataset_id,
           D.DATASET this_dataset
        FROM {{.Owner}}.DATASETS PD
        JOIN {{.Owner}}.DATASET_PARENTS DP ON DP.PARENT_DATASET_ID = PD.DATASET_ID
        JOIN {{.Owner}}.DATASETS D ON  D.DATASET_ID = DP.THIS_DATASET_ID
