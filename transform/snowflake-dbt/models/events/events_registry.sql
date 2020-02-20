{{config({
    "materialized": "incremental",
    "schema": "events"
  })
}}

WITH events          AS (
    SELECT
        LOWER(type)                                                                   AS event_name
      , CASE WHEN lower(category) = 'actions' THEN 'action' ELSE lower(category) END  AS event_category
      , MIN(timestamp::date)                                                          AS date_added
    FROM {{ source('mattermost2', 'event')}}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE LOWER(type) NOT IN (SELECT event_name FROM {{ this }} GROUP BY 1)

    {% endif %}
    GROUP BY 1, 2
),
     events_registry AS (
         SELECT
             uuid_string()                                                                AS uuid
           , event_name
           , event_category
           , 'THIS IS A PLACEHOLDER DESCRIPTION. IT IS ONLY MEANT TO ESTABLISH THE MINIMUM LENGTH OF THIS FIELD. 
              THIS FIELD IS MEANT TO BE USED TO PROVIDE A BRIEF DESCRIPTION OF THE EVENT IN QUESTION 
              I.E. HOW THE EVENT IS TRIGGERED AND WHY IT FALLS INTO A SPECIFIC CATEGORY.' AS description
           , date_added
         FROM events
         GROUP BY 1, 2, 3, 4, 5
     )
SELECT *
FROM events_registry