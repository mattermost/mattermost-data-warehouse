UPDATE sales.scrub_segment
SET
    nn_forecast = COALESCE(recent_updates.nn_forecast, scrub_segment.nn_forecast),
    nn_upside = COALESCE(recent_updates.nn_upside, scrub_segment.nn_upside),
    ren_forecast = COALESCE(recent_updates.ren_forecast, scrub_segment.ren_forecast),
    ren_upside = COALESCE(recent_updates.ren_upside, scrub_segment.ren_upside)
FROM
(
    SELECT qtr,
           segment,
           MAX(CASE WHEN field = 'nn_forecast' THEN new_value ELSE NULL END) AS nn_forecast,
           MAX(CASE WHEN field = 'nn_upside' THEN new_value ELSE NULL END) AS nn_upside,
           MAX(CASE WHEN field = 'ren_forecast' THEN new_value ELSE NULL END) AS ren_forecast,
           MAX(CASE WHEN field = 'ren_upside' THEN new_value ELSE NULL END) AS ren_upside
    FROM (
             SELECT ROW_NUMBER()OVER (PARTITION BY PARSE_JSON(other_params): qtr, PARSE_JSON(other_params):scrub_segment, field ORDER BY triggered_at DESC) AS row_num,
                    PARSE_JSON(other_params): qtr::varchar AS qtr,
                    PARSE_JSON(other_params):scrub_segment::varchar AS segment,
                    field,
                    new_value
             FROM zapier_data_actions.data
             WHERE table_name = 'scrub_segment' AND field IN ('nn_upside', 'nn_forecast', 'ren_upside', 'ren_forecast') AND dwh_processed_at IS NULL
         )
    WHERE row_num = 1
    GROUP BY 1, 2
) AS recent_updates
WHERE scrub_segment.qtr = recent_updates.qtr AND scrub_segment.segment = recent_updates.segment;

UPDATE sales.commit_segment
SET
    commit_netnew = COALESCE(recent_updates.nn_forecast, commit_segment.commit_netnew),
    upside_netnew = COALESCE(recent_updates.nn_upside, commit_segment.upside_netnew),
    commit_renewal = COALESCE(recent_updates.ren_forecast, commit_segment.commit_renewal),
    upside_renewal = COALESCE(recent_updates.ren_upside, commit_segment.upside_renewal)
FROM
(
    SELECT qtr,
           segment,
           MAX(CASE WHEN field = 'nn_forecast' THEN new_value ELSE NULL END) AS nn_forecast,
           MAX(CASE WHEN field = 'nn_upside' THEN new_value ELSE NULL END) AS nn_upside,
           MAX(CASE WHEN field = 'ren_forecast' THEN new_value ELSE NULL END) AS ren_forecast,
           MAX(CASE WHEN field = 'ren_upside' THEN new_value ELSE NULL END) AS ren_upside
    FROM (
             SELECT ROW_NUMBER()OVER (PARTITION BY PARSE_JSON(other_params): qtr, PARSE_JSON(other_params):scrub_segment, field ORDER BY triggered_at DESC) AS row_num,
                    PARSE_JSON(other_params): qtr::varchar AS qtr,
                    PARSE_JSON(other_params):scrub_segment::varchar AS segment,
                    field,
                    new_value
             FROM zapier_data_actions.data
             WHERE table_name = 'scrub_segment' AND field IN ('nn_upside', 'nn_forecast', 'ren_upside', 'ren_forecast') AND dwh_processed_at IS NULL
         )
    WHERE row_num = 1
    GROUP BY 1, 2
) AS recent_updates
WHERE commit_segment.qtr = recent_updates.qtr AND commit_segment.segment = recent_updates.segment;

UPDATE sales.scrub_ww
SET
    nn_forecast = COALESCE(recent_updates.nn_forecast, scrub_ww.nn_forecast),
    nn_upside = COALESCE(recent_updates.nn_upside, scrub_ww.nn_upside),
    ren_forecast = COALESCE(recent_updates.ren_forecast, scrub_ww.ren_forecast),
    ren_upside = COALESCE(recent_updates.ren_upside, scrub_ww.ren_upside)
FROM
(
    SELECT qtr,
           MAX(CASE WHEN field = 'nn_forecast' THEN new_value ELSE NULL END) AS nn_forecast,
           MAX(CASE WHEN field = 'nn_upside' THEN new_value ELSE NULL END) AS nn_upside,
           MAX(CASE WHEN field = 'ren_forecast' THEN new_value ELSE NULL END) AS ren_forecast,
           MAX(CASE WHEN field = 'ren_upside' THEN new_value ELSE NULL END) AS ren_upside
    FROM (
             SELECT ROW_NUMBER()OVER (PARTITION BY PARSE_JSON(other_params): qtr, field ORDER BY triggered_at DESC) AS row_num,
                    PARSE_JSON(other_params): qtr::varchar AS qtr,
                    field,
                    new_value
             FROM zapier_data_actions.data
             WHERE table_name = 'scrub_ww' AND field IN ('nn_upside', 'nn_forecast', 'ren_upside', 'ren_forecast') AND dwh_processed_at IS NULL
         )
    WHERE row_num = 1
    GROUP BY 1
) AS recent_updates
WHERE scrub_ww.qtr = recent_updates.qtr;

UPDATE sales.commit_ww
SET
    commit_netnew = COALESCE(recent_updates.nn_forecast, commit_ww.commit_netnew),
    upside_netnew = COALESCE(recent_updates.nn_upside, commit_ww.upside_netnew),
    commit_renewal = COALESCE(recent_updates.ren_forecast, commit_ww.commit_renewal),
    upside_renewal = COALESCE(recent_updates.ren_upside, commit_ww.upside_renewal)
FROM
(
    SELECT qtr,
           MAX(CASE WHEN field = 'nn_forecast' THEN new_value ELSE NULL END) AS nn_forecast,
           MAX(CASE WHEN field = 'nn_upside' THEN new_value ELSE NULL END) AS nn_upside,
           MAX(CASE WHEN field = 'ren_forecast' THEN new_value ELSE NULL END) AS ren_forecast,
           MAX(CASE WHEN field = 'ren_upside' THEN new_value ELSE NULL END) AS ren_upside
    FROM (
             SELECT ROW_NUMBER()OVER (PARTITION BY PARSE_JSON(other_params): qtr, field ORDER BY triggered_at DESC) AS row_num,
                    PARSE_JSON(other_params): qtr::varchar AS qtr,
                    field,
                    new_value
             FROM zapier_data_actions.data
             WHERE table_name = 'scrub_ww' AND field IN ('nn_upside', 'nn_forecast', 'ren_upside', 'ren_forecast') AND dwh_processed_at IS NULL
         )
    WHERE row_num = 1
    GROUP BY 1
) AS recent_updates
WHERE commit_ww.qtr = recent_updates.qtr;

UPDATE zapier_data_actions.data
SET dwh_processed_at = current_timestamp()
WHERE dwh_processed_at IS NULL;
