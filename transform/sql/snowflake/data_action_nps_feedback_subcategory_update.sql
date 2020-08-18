INSERT INTO analytics.mattermost.nps_feedback_classification(last_feedback_date, server_id, user_id, subcategory, categorized_at, categorized_by, manually_categorized)
        SELECT 
            last_feedback_date
          , server_id
          , user_id
          , new_value as subcategory
          , triggered_at as categorized_at
          , triggered_by as categorized_by
          , TRUE as manually_categorized
        FROM (
                SELECT 
                    ROW_NUMBER() OVER 
                        (PARTITION BY PARSE_JSON(other_params):last_feedback_date, PARSE_JSON(other_params):user_id, PARSE_JSON(other_params):server_id, field ORDER BY triggered_at DESC) AS row_num
                  , PARSE_JSON(other_params):user_id::varchar AS user_id
                  , PARSE_JSON(other_params):server_id::varchar AS server_id
                  , PARSE_JSON(other_params):last_feedback_date::date AS last_feedback_date
                  , field
                  , new_value
                  , triggered_by
                  , triggered_at
                FROM zapier_data_actions.data
                WHERE table_name = 'nps_feedback_classification' AND field IN ('subcategory') AND dwh_processed_at IS NULL
            )
        WHERE row_num = 1
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    ;

UPDATE zapier_data_actions.data
SET dwh_processed_at = current_timestamp()
WHERE dwh_processed_at IS NULL AND table_name IN ('nps_feedback_classification') and field IN ('subcategory');