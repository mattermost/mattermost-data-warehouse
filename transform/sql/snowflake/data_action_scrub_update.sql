UPDATE sales.scrub_segment
SET
    nn_forecast = CASE WHEN field = 'nn_forecast' THEN new_value ELSE nn_forecast END,
    nn_upside = CASE WHEN field = 'nn_upside' THEN new_value ELSE nn_upside END,
    ren_forecast = CASE WHEN field = 'ren_forecast' THEN new_value ELSE ren_forecast END,
    ren_upside = CASE WHEN field = 'ren_upside' THEN new_value ELSE ren_upside END
FROM
(
    SELECT ROW_NUMBER () OVER (PARTITION BY PARSE_JSON(other_params): qtr, PARSE_JSON(other_params):scrub_segment, field ORDER BY triggered_at DESC) AS row_num,
           PARSE_JSON(other_params):qtr::varchar AS qtr,
           PARSE_JSON(other_params):scrub_segment::varchar AS segment,
           field,
           new_value
    FROM zapier_data_actions.data
    WHERE table_name = 'scrub_segment'
      AND field IN ('nn_upside', 'nn_forecast', 'ren_upside', 'ren_forecast')
      AND dwh_processed_at IS NULL
) AS recent_updates
WHERE row_num = 1
    AND scrub_segment.qtr = recent_updates.qtr
    AND scrub_segment.segment = recent_updates.segment;

UPDATE sales.scrub_ww
SET
    nn_forecast = CASE WHEN field = 'nn_forecast' THEN new_value ELSE nn_forecast END,
    nn_upside = CASE WHEN field = 'nn_upside' THEN new_value ELSE nn_upside END,
    ren_forecast = CASE WHEN field = 'ren_forecast' THEN new_value ELSE ren_forecast END,
    ren_upside = CASE WHEN field = 'ren_upside' THEN new_value ELSE ren_upside END
FROM
(
    SELECT ROW_NUMBER () OVER (PARTITION BY PARSE_JSON(other_params): qtr, field ORDER BY triggered_at DESC) AS row_num,
           PARSE_JSON(other_params):qtr::varchar AS qtr,
           field,
           new_value
    FROM zapier_data_actions.data
    WHERE table_name = 'scrub_ww'
      AND field IN ('nn_upside', 'nn_forecast', 'ren_upside', 'ren_forecast')
      AND dwh_processed_at IS NULL
) AS recent_updates
WHERE row_num = 1
    AND scrub_segment.qtr = recent_updates.qtr;

UPDATE zapier_data_actions.data
SET dwh_processed_at = now()
WHERE dwh_processed_at IS NULL;
