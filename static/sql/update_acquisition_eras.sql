UPDATE {{.Owner}}.ACQUISITION_ERAS
    SET END_DATE=:end_date
    WHERE acquisition_era_name=:acquisition_era_name
