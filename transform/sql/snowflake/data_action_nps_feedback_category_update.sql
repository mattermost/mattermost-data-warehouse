MERGE INTO analytics.mattermost.nps_feedback_classification
    USING (
        SELECT 
            date
          , id
          , user_id
          , server_id
          , field
          , new_value as category
        FROM (
                SELECT 
                    ROW_NUMBER() OVER 
                        (PARTITION BY PARSE_JSON(other_params):date, PARSE_JSON(other_params):id, field ORDER BY triggered_at DESC) AS row_num
                  , PARSE_JSON(other_params):user_id::varchar AS user_id
                  , PARSE_JSON(other_params):server_id::varchar AS server_id
                  , PARSE_JSON(other_params):date::date AS date
                  , PARSE_JSON(other_params):id::varchar AS id
                  , field
                  , new_value
                FROM zapier_data_actions.data
                WHERE table_name = 'nps_feedback_classification' AND field IN ('category') AND dwh_processed_at IS NULL
            )
        WHERE row_num = 1
        GROUP BY 1, 2, 3, 4, 5, 6
    ) AS recent_updates
    ON nps_feedback_classification.id = recent_updates.id
WHEN MATCHED THEN UPDATE
    SET nps_feedback_classification.category = recent_updates.category
WHEN NOT MATCHED THEN
    INSERT(date, server_id, user_id, id, category) values (recent_updates.date, recent_updates.server_id, recent_updates.user_id, recent_updates.id, recent_updates.category);

UPDATE zapier_data_actions.data
SET dwh_processed_at = current_timestamp()
WHERE dwh_processed_at IS NULL AND table_name IN ('nps_feedback_classification') and field IN ('category');