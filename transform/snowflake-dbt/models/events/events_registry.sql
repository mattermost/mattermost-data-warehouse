{{config({
    "materialized": "incremental",
    "schema": "events",
    "unique_key": "id"
  })
}}

WITH events          AS (
    SELECT
        LOWER(type)                                                                   AS event_name
      , CASE WHEN lower(category) = 'actions' THEN 'action' ELSE lower(category) END  AS event_category
      , MIN(timestamp::date)                                                          AS date_added
      , MAX(timestamp::date)                                                          AS last_triggered
    FROM {{ source('mattermost2', 'event')}}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE LOWER(type) NOT IN (SELECT event_name FROM {{ this }} GROUP BY 1)

    {% endif %}
    GROUP BY 1, 2
),

    mobile_events    AS (
      SELECT
        LOWER(type)                                                                   AS event_name
      , CASE WHEN lower(category) = 'actions' THEN 'action' ELSE lower(category) END  AS event_category
      , MIN(timestamp::date)                                                          AS date_added
      , MAX(timestamp::date)                                                          AS last_triggered
    FROM {{ source('mattermost2', 'event_mobile')}}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE LOWER(type) NOT IN (SELECT event_name FROM {{ this }} GROUP BY 1)

    {% endif %}
    GROUP BY 1, 2
    ),

    all_events       AS (
      SELECT *
      FROM events
      UNION ALL
      SELECT * 
      FROM mobile_events
    ),

     events_registry AS (
         SELECT
             {{ dbt_utils.surrogate_key('event_name', 'event_category') }}                AS event_id
           , event_name
           , event_category
           , 'THIS IS A PLACEHOLDER DESCRIPTION. IT IS ONLY MEANT TO ESTABLISH THE MINIMUM LENGTH OF THIS FIELD. 
              THIS FIELD IS MEANT TO BE USED TO PROVIDE A BRIEF DESCRIPTION OF THE EVENT IN QUESTION 
              I.E. HOW THE EVENT IS TRIGGERED AND WHY IT FALLS INTO A SPECIFIC CATEGORY.' AS description
           , MIN(date_added)                                                              AS date_added
           , MAX(last_triggered)                                                          AS last_triggered
         FROM all_events
         GROUP BY 1, 2, 3, 4
     )
SELECT *
FROM events_registry