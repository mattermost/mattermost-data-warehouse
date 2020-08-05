UPDATE mattermost.nps_feedback_classification
    SET nps_feedback_classification.subcategory = recent_updates.subcategory
    FROM (
        SELECT date,
            id,
            user_id,
            server_id,
            field,
            new_value as subcategory
        FROM (
                SELECT ROW_NUMBER()OVER (PARTITION BY PARSE_JSON(other_params):date, PARSE_JSON(other_params):id, field ORDER BY triggered_at DESC) AS row_num,
                        PARSE_JSON(other_params):user_id::varchar AS user_id,
                        PARSE_JSON(other_params):server_id::varchar AS server_id,
                        PARSE_JSON(other_params):date::date AS date,
                        PARSE_JSON(other_params):id::varchar AS id,
                        field,
                        new_value
                FROM zapier_data_actions.data
                WHERE table_name = 'nps_feedback_classification' AND field IN ('subcategory') AND dwh_processed_at IS NULL
            )
        WHERE row_num = 1
        GROUP BY 1, 2
    ) AS recent_updates
    WHERE nps_feedback_classification.id = recent_updates.id;

UPDATE zapier_data_actions.data
SET dwh_processed_at = current_timestamp()
WHERE dwh_processed_at IS NULL AND table_name IN ('nps_feedback_classification') and field IN ('subcategory');