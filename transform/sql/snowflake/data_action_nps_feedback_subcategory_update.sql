INSERT INTO analytics.mattermost.nps_feedback_classification(last_feedback_date, server_id, user_id, subcategory, categorized_at, categorized_by, manually_categorized, manually_categorized_id)
    SELECT 
        last_feedback_date
    , server_id
    , user_id
    , new_value as subcategory
    , triggered_at as categorized_at
    , triggered_by as categorized_by
    , TRUE as manually_categorized
    , data_action_id as manually_categorized_id
    FROM (
            SELECT
                ROW_NUMBER() OVER
                    (PARTITION BY PARSE_JSON(data):last_feedback_date, PARSE_JSON(data):user_id, PARSE_JSON(data):server_id, PARSE_JSON(data):field ORDER BY PARSE_JSON(data):triggered_at DESC) AS row_num
            , PARSE_JSON(data):user_id::varchar AS user_id
            , PARSE_JSON(data):server_id::varchar AS server_id
            , PARSE_JSON(data):last_feedback_date::date AS last_feedback_date
            , PARSE_JSON(data):field_name::varchar AS field_name
            , PARSE_JSON(FORM_PARAMS):new_value::varchar as new_value
            , _SDC_RECEIVED_AT::timestamp as triggered_at
            , PARSE_JSON(data):action_performed_by::varchar as triggered_by
            , __SDC_PRIMARY_KEY as data_action_id
            FROM LOOKER_DATA_ACTIONS.data
            WHERE PARSE_JSON(data):table = 'nps_feedback_classification' AND PARSE_JSON(data):field_name IN ('subcategory')
        ) as data_action_data
    WHERE row_num = 1
        AND NOT EXISTS (SELECT 1 FROM analytics.mattermost.nps_feedback_classification WHERE manually_categorized_id = data_action_data.data_action_id)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;